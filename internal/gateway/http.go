package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"strings"
	"time"
)

const Path = "/v1/chat/completions"

func Handler(worker string, q *Queue, timeout time.Duration) http.Handler {
	return HandlerWithBackup(worker, "", q, timeout)
}

func send(ctx context.Context, address string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(address, "/")+Path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	return http.DefaultClient.Do(req)
}

// retryOnBackup reports whether a primary status should fail over. 500 is included because vLLM
// answers in-flight requests with 500 while shutting down, before sending any tokens.
func retryOnBackup(status int) bool {
	return status == 500 || status == 502 || status == 503 || status == 504
}

func HandlerWithBackup(worker, backup string, q *Queue, timeout time.Duration) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "ok") })
	mux.HandleFunc("POST "+Path, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			http.Error(w, "could not read request (1 MiB limit)", 400)
			return
		}
		var input struct{ Stream bool }
		if json.Unmarshal(body, &input) != nil || !input.Stream {
			http.Error(w, "expected JSON with stream=true", 400)
			return
		}
		p := r.Header.Get("X-Request-Priority")
		if p != "" && p != "urgent" && p != "background" {
			http.Error(w, "invalid priority", 400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		if err = q.Acquire(ctx, p == "urgent"); err != nil {
			if errors.Is(err, ErrFull) {
				http.Error(w, err.Error(), 429)
			} else {
				http.Error(w, "expired while queued", 504)
			}
			return
		}
		defer q.Release()
		role := "primary"
		res, err := send(ctx, worker, body)
		// Nothing has been written to the client yet, so a failed primary can be retried on the backup once.
		if backup != "" && ctx.Err() == nil && (err != nil || retryOnBackup(res.StatusCode)) {
			if res != nil {
				res.Body.Close()
			}
			role = "backup"
			res, err = send(ctx, backup, body)
		}
		if err != nil {
			status := 502
			if ctx.Err() == context.DeadlineExceeded {
				status = 504
			}
			http.Error(w, err.Error(), status)
			return
		}
		defer res.Body.Close()
		if res.StatusCode != 200 {
			w.WriteHeader(res.StatusCode)
			io.Copy(w, io.LimitReader(res.Body, 8192))
			return
		}
		media, _, _ := mime.ParseMediaType(res.Header.Get("Content-Type"))
		if media != "text/event-stream" {
			http.Error(w, "expected an SSE response from worker", 502)
			return
		}
		w.Header().Set("X-Inference-Worker", role)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		ctrl := http.NewResponseController(w)
		lines := bufio.NewScanner(res.Body)
		lines.Buffer(make([]byte, 4096), 1<<20)
		done := false
		for lines.Scan() {
			line := lines.Text()
			if _, err = fmt.Fprintln(w, line); err != nil {
				return
			}
			if line == "" {
				if ctrl.Flush() != nil {
					return
				}
				if done {
					return
				}
			} else if strings.TrimSpace(line) == "data: [DONE]" {
				done = true
			}
		}
		if r.Context().Err() == nil {
			log.Printf("worker stream ended before complete DONE event: %v", lines.Err())
		}
		// Headers may already have reached the client. Abort the transport on truncation.
		panic(http.ErrAbortHandler)
	})
	return mux
}
