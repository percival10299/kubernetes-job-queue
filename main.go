package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type job struct {
	id      int64
	payload string
}

func claim(ctx context.Context, db *sql.DB, workerID string) (job, bool, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return job{}, false, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	j, claimed, err := claimInTx(ctx, tx, workerID)
	if err != nil {
		return job{}, false, err
	}
	if !claimed {
		if err := tx.Rollback(); err != nil {
			return job{}, false, fmt.Errorf("roll back empty claim: %w", err)
		}
		return job{}, false, nil
	}
	if err := tx.Commit(); err != nil {
		return job{}, false, fmt.Errorf("commit claim: %w", err)
	}
	return j, true, nil
}

func claimInTx(ctx context.Context, tx *sql.Tx, workerID string) (job, bool, error) {
	var j job
	err := tx.QueryRowContext(ctx,
		"SELECT id, payload FROM jobs WHERE status = 'queued' ORDER BY id LIMIT 1 FOR UPDATE",
	).Scan(&j.id, &j.payload)
	if errors.Is(err, sql.ErrNoRows) {
		return job{}, false, nil
	}
	if err != nil {
		return job{}, false, fmt.Errorf("select queued job: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		"UPDATE jobs SET status = 'running', claimed_by = ? WHERE id = ?",
		workerID, j.id,
	); err != nil {
		return job{}, false, fmt.Errorf("update job: %w", err)
	}
	return j, true, nil
}

func complete(ctx context.Context, db *sql.DB, workerID string, j job) error {
	hash := sha256.Sum256([]byte(j.payload))
	result, err := db.ExecContext(ctx,
		"UPDATE jobs SET status = 'succeeded', result = ? WHERE id = ? AND status = 'running' AND claimed_by = ?",
		fmt.Sprintf("%x", hash), j.id, workerID)
	if err != nil {
		return fmt.Errorf("complete job %d: %w", j.id, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check completion of job %d: %w", j.id, err)
	}
	if affected != 1 {
		return fmt.Errorf("complete job %d: no longer claimed by %s", j.id, workerID)
	}
	return nil
}

func run() error {
	workerID := flag.String("id", "", "worker identifier (required)")
	flag.Parse()
	if strings.TrimSpace(*workerID) == "" {
		return errors.New("--id is required")
	}
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return errors.New("MYSQL_DSN is required")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	ctx := context.Background()
	j, claimed, err := claim(ctx, db, *workerID)
	if err != nil {
		return err
	}
	if !claimed {
		fmt.Println("no job available")
	} else {
		fmt.Printf("claimed job %d\n", j.id)
		if err := complete(ctx, db, *workerID, j); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
