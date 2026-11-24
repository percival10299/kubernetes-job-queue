package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kubernetes-job-queue/internal/api"
)

func TestSubmitProcessAndRetrieve(t *testing.T) {
	db, dsn := testDatabase(t)
	server := httptest.NewServer(api.Handler(db))
	defer server.Close()

	binary := filepath.Join(t.TempDir(), "worker")
	if output, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v\n%s", err, output)
	}

	for _, tc := range []struct {
		body string
		text string
		hash string
	}{
		{`{"text":"hello"}`, "hello", "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"},
		{`{"text":""}`, "", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{`{"text":"caf\u00e9 \ud83d\udc31"}`, "caf\u00e9 \U0001f431", "039dbbbd11c6b37ca42feecc86da054a8bd8da69f39d3068e1b6375b2f4368e0"},
	} {
		resp, err := http.Post(server.URL+"/jobs", "application/json", strings.NewReader(tc.body))
		if err != nil {
			t.Fatalf("submit job: %v", err)
		}
		var submitted struct {
			ID int64 `json:"id"`
		}
		err = json.NewDecoder(resp.Body).Decode(&submitted)
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted || err != nil || submitted.ID <= 0 {
			t.Fatalf("submit job: status=%d id=%d decode=%v", resp.StatusCode, submitted.ID, err)
		}
		var status, payload string
		var result sql.NullString
		if err := db.QueryRow("SELECT status, payload, result FROM jobs WHERE id = ?", submitted.ID).Scan(&status, &payload, &result); err != nil {
			t.Fatalf("read submitted job: %v", err)
		}
		if status != "queued" || payload != tc.text || result.Valid {
			t.Fatalf("submitted job: status=%q payload=%q result=%+v", status, payload, result)
		}
		url := fmt.Sprintf("%s/jobs/%d", server.URL, submitted.ID)
		assertJobResponse(t, url, "queued", "")

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		cmd := exec.CommandContext(ctx, binary, "--id", "integration-worker")
		cmd.Env = append(os.Environ(), "MYSQL_DSN="+dsn)
		output, err := cmd.CombinedOutput()
		cancel()
		if err != nil || strings.TrimSpace(string(output)) != fmt.Sprintf("claimed job %d", submitted.ID) {
			t.Fatalf("run worker: %v, output=%q", err, output)
		}
		assertJobResponse(t, url, "succeeded", tc.hash)
	}
}

func assertJobResponse(t *testing.T, url, status, result string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	defer resp.Body.Close()
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode job response: %v", err)
	}
	if resp.StatusCode != http.StatusOK || body["status"] != status {
		t.Fatalf("get job: status=%d body=%v; want status %q", resp.StatusCode, body, status)
	}
	value, present := body["result"]
	if (result == "" && present) || (result != "" && (!present || value != result)) {
		t.Fatalf("get job result: %v; want %q", body, result)
	}
}

func TestInvalidJobRequests(t *testing.T) {
	db, _ := testDatabase(t)
	server := httptest.NewServer(api.Handler(db))
	defer server.Close()
	for _, body := range []string{`{}`, `{"text":null}`, `{"text":42}`, `{"text":"a"} {}`} {
		resp, err := http.Post(server.URL+"/jobs", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("submit invalid job: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("POST %s: got status %d, want 400", body, resp.StatusCode)
		}
	}
	for _, tc := range []struct {
		path string
		code int
	}{
		{"/jobs/invalid", http.StatusBadRequest},
		{"/jobs/0", http.StatusBadRequest},
		{"/jobs/999", http.StatusNotFound},
	} {
		resp, err := http.Get(server.URL + tc.path)
		if err != nil {
			t.Fatalf("get %s: %v", tc.path, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != tc.code {
			t.Errorf("GET %s: got status %d, want %d", tc.path, resp.StatusCode, tc.code)
		}
	}
}
