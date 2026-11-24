package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
)

func Handler(db *sql.DB) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Text *string `json:"text"`
		}
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&input); err != nil || input.Text == nil {
			http.Error(w, "expected a JSON object with a text string", http.StatusBadRequest)
			return
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			http.Error(w, "expected a single JSON object", http.StatusBadRequest)
			return
		}

		result, err := db.ExecContext(r.Context(),
			"INSERT INTO jobs (status, payload) VALUES ('queued', ?)", *input.Text)
		if err != nil {
			log.Printf("insert job: %v", err)
			http.Error(w, "could not submit job", http.StatusInternalServerError)
			return
		}
		id, err := result.LastInsertId()
		if err != nil {
			log.Printf("get submitted job ID: %v", err)
			http.Error(w, "could not submit job", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(w).Encode(struct {
			ID int64 `json:"id"`
		}{ID: id}); err != nil {
			log.Printf("write submitted job response: %v", err)
		}
	})
	mux.HandleFunc("GET /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil || id < 1 {
			http.Error(w, "invalid job ID", http.StatusBadRequest)
			return
		}
		var status string
		var result sql.NullString
		err = db.QueryRowContext(r.Context(),
			"SELECT status, result FROM jobs WHERE id = ?", id).Scan(&status, &result)
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}
		if err != nil {
			log.Printf("get job %d: %v", id, err)
			http.Error(w, "could not retrieve job", http.StatusInternalServerError)
			return
		}
		response := struct {
			Status string  `json:"status"`
			Result *string `json:"result,omitempty"`
		}{Status: status}
		if result.Valid {
			response.Result = &result.String
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Printf("write job %d response: %v", id, err)
		}
	})
	return mux
}
