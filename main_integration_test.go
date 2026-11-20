package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func testDatabase(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set MYSQL_TEST_DSN to run MySQL integration tests")
	}
	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse test DSN: %v", err)
	}
	config.DBName = ""
	admin, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		t.Fatalf("open MySQL: %v", err)
	}
	t.Cleanup(func() {
		if err := admin.Close(); err != nil {
			t.Errorf("close MySQL admin connection: %v", err)
		}
	})

	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatalf("generate database name: %v", err)
	}
	name := "job_queue_test_" + hex.EncodeToString(suffix[:])
	if _, err := admin.Exec("CREATE DATABASE `" + name + "`"); err != nil {
		t.Fatalf("create test database: %v", err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec("DROP DATABASE `" + name + "`"); err != nil {
			t.Errorf("drop test database %s: %v", name, err)
		}
	})

	config.DBName = name
	testDSN := config.FormatDSN()
	db, err := sql.Open("mysql", testDSN)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close test database: %v", err)
		}
	})

	schema, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	for _, statement := range strings.Split(string(schema), ";") {
		if statement = strings.TrimSpace(statement); statement != "" {
			if _, err := db.Exec(statement); err != nil {
				t.Fatalf("initialize test database: %v", err)
			}
		}
	}
	return db, testDSN
}

func testWorkerRace(t *testing.T, jobs, workers int) {
	t.Helper()
	db, dsn := testDatabase(t)
	for i := 1; i < jobs; i++ {
		if _, err := db.Exec("INSERT INTO jobs (status) VALUES ('queued')"); err != nil {
			t.Fatalf("seed job: %v", err)
		}
	}

	binary := filepath.Join(t.TempDir(), "worker")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v\n%s", err, output)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	type result struct {
		worker string
		output string
		err    error
	}
	results := make(chan result, workers)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(workers)
	for i := 0; i < workers; i++ {
		worker := fmt.Sprintf("worker-%d", i+1)
		go func() {
			ready.Done()
			<-start
			cmd := exec.CommandContext(ctx, binary, "--id", worker)
			cmd.Env = append(os.Environ(), "MYSQL_DSN="+dsn)
			output, err := cmd.CombinedOutput()
			results <- result{worker, strings.TrimSpace(string(output)), err}
		}()
	}
	ready.Wait()
	close(start)

	claimed := make(map[int64]string)
	noJob := 0
	for i := 0; i < workers; i++ {
		r := <-results
		if r.err != nil {
			t.Errorf("%s failed: %v: %s", r.worker, r.err, r.output)
			continue
		}
		if r.output == "no job available" {
			noJob++
			continue
		}
		idText, ok := strings.CutPrefix(r.output, "claimed job ")
		id, err := strconv.ParseInt(idText, 10, 64)
		if !ok || err != nil {
			t.Errorf("%s produced unexpected output: %q", r.worker, r.output)
			continue
		}
		if previous, exists := claimed[id]; exists {
			t.Errorf("job %d claimed by both %s and %s", id, previous, r.worker)
		}
		claimed[id] = r.worker
	}
	if len(claimed) != jobs || noJob != workers-jobs {
		t.Errorf("got %d distinct claims and %d unavailable; want %d and %d",
			len(claimed), noJob, jobs, workers-jobs)
	}

	rows, err := db.Query("SELECT id, status, claimed_by FROM jobs ORDER BY id")
	if err != nil {
		t.Fatalf("query jobs: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id int64
		var status string
		var owner sql.NullString
		if err := rows.Scan(&id, &status, &owner); err != nil {
			t.Fatalf("scan job: %v", err)
		}
		count++
		if status != "running" || !owner.Valid || claimed[id] != owner.String {
			t.Errorf("job %d: status=%q owner=%q; reported owner=%q",
				id, status, owner.String, claimed[id])
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read jobs: %v", err)
	}
	if count != jobs {
		t.Errorf("got %d rows, want %d", count, jobs)
	}
}

func TestOneJobTwoWorkers(t *testing.T) {
	testWorkerRace(t, 1, 2)
}

func TestTenJobsTwelveWorkers(t *testing.T) {
	testWorkerRace(t, 10, 12)
}

func TestRolledBackClaimCanBeReclaimed(t *testing.T) {
	db, _ := testDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer tx.Rollback()

	firstID, claimed, err := claimInTx(ctx, tx, "worker-a")
	if err != nil || !claimed {
		t.Fatalf("first claim: id=%d claimed=%t err=%v", firstID, claimed, err)
	}
	var status, owner string
	if err := tx.QueryRowContext(ctx, "SELECT status, claimed_by FROM jobs WHERE id = ?", firstID).Scan(&status, &owner); err != nil {
		t.Fatalf("read uncommitted claim: %v", err)
	}
	if status != "running" || owner != "worker-a" {
		t.Fatalf("uncommitted job: status=%q owner=%q", status, owner)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback claim: %v", err)
	}

	secondID, claimed, err := claim(ctx, db, "worker-b")
	if err != nil || !claimed || secondID != firstID {
		t.Fatalf("second claim: id=%d claimed=%t err=%v; want id=%d", secondID, claimed, err, firstID)
	}
	if err := db.QueryRowContext(ctx, "SELECT status, claimed_by FROM jobs WHERE id = ?", firstID).Scan(&status, &owner); err != nil {
		t.Fatalf("read committed claim: %v", err)
	}
	if status != "running" || owner != "worker-b" {
		t.Errorf("committed job: status=%q owner=%q", status, owner)
	}
}
