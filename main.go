package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func claim(ctx context.Context, db *sql.DB, workerID string) (int64, bool, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	id, claimed, err := claimInTx(ctx, tx, workerID)
	if err != nil {
		return 0, false, err
	}
	if !claimed {
		if err := tx.Rollback(); err != nil {
			return 0, false, fmt.Errorf("roll back empty claim: %w", err)
		}
		return 0, false, nil
	}
	if err := tx.Commit(); err != nil {
		return 0, false, fmt.Errorf("commit claim: %w", err)
	}
	return id, true, nil
}

func claimInTx(ctx context.Context, tx *sql.Tx, workerID string) (int64, bool, error) {
	var id int64
	err := tx.QueryRowContext(ctx,
		"SELECT id FROM jobs WHERE status = 'queued' ORDER BY id LIMIT 1 FOR UPDATE",
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("select queued job: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		"UPDATE jobs SET status = 'running', claimed_by = ? WHERE id = ?",
		workerID, id,
	); err != nil {
		return 0, false, fmt.Errorf("update job: %w", err)
	}
	return id, true, nil
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

	id, claimed, err := claim(context.Background(), db, *workerID)
	if err != nil {
		return err
	}
	if !claimed {
		fmt.Println("no job available")
	} else {
		fmt.Printf("claimed job %d\n", id)
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
