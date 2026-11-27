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

// readRequest validates a streaming chat request. On failure it has already answered the client.
func readRequest(w http.ResponseWriter, r *http.Request) (body []byte, urgent, ok bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		http.Error(w, "could not read request (1 MiB limit)", 400)
		return nil, false, false
	}
	var input struct{ Stream bool }
	if json.Unmarshal(body, &input) != nil || !input.Stream {
		http.Error(w, "expected JSON with stream=true", 400)
		return nil, false, false
	}
	p := r.Header.Get("X-Request-Priority")
	if p != "" && p != "urgent" && p != "background" {
		http.Error(w, "invalid priority", 400)
		return nil, false, false
	}
	return body, p == "urgent", true
}

func sendError(w http.ResponseWriter, ctx context.Context, err error) {
	status := 502
	if ctx.Err() == context.DeadlineExceeded {
		status = 504
	}
	http.Error(w, err.Error(), status)
}

// passResponse relays a worker's non-200 answer, or rejects a 200 that is not SSE. It returns false
// when the response was already answered and must not be streamed.
func passResponse(w http.ResponseWriter, res *http.Response) bool {
	if res.StatusCode != 200 {
		w.WriteHeader(res.StatusCode)
		io.Copy(w, io.LimitReader(res.Body, 8192))
		return false
	}
	media, _, _ := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if media != "text/event-stream" {
		http.Error(w, "expected an SSE response from worker", 502)
		return false
	}
	return true
}

type relayEnd int

const (
	relayDone       relayEnd = iota // Complete [DONE] event forwarded.
	relayClientGone                 // Writing or flushing to the client failed.
	relayTruncated                  // Worker stream ended before a complete [DONE] event.
)

// hasContent reports whether an SSE line carries a nonempty content delta (not the role-only event).
func hasContent(line string) bool {
	data, ok := strings.CutPrefix(line, "data:")
	if !ok || !strings.Contains(data, "content") {
		return false
	}
	var chunk struct {
		Choices []struct {
			Delta struct{ Content string } `json:"delta"`
			Text  string                   `json:"text"`
		} `json:"choices"`
	}
	if json.Unmarshal([]byte(data), &chunk) != nil {
		return false
	}
	for _, c := range chunk.Choices {
		if c.Delta.Content != "" || c.Text != "" {
			return true
		}
	}
	return false
}

// relay streams the worker's SSE response, flushing at each event boundary. onContent, if set,
// runs once at the first content event.
func relay(w http.ResponseWriter, res *http.Response, role string, onContent func()) relayEnd {
	w.Header().Set("X-Inference-Worker", role)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	ctrl := http.NewResponseController(w)
	lines := bufio.NewScanner(res.Body)
	lines.Buffer(make([]byte, 4096), 1<<20)
	done := false
	for lines.Scan() {
		line := lines.Text()
		if _, err := fmt.Fprintln(w, line); err != nil {
			return relayClientGone
		}
		if onContent != nil && hasContent(line) {
			onContent()
			onContent = nil
		}
		if line == "" {
			if ctrl.Flush() != nil {
				return relayClientGone
			}
			if done {
				return relayDone
			}
		} else if strings.TrimSpace(line) == "data: [DONE]" {
			done = true
		}
	}
	return relayTruncated
}

func HandlerWithBackup(worker, backup string, q *Queue, timeout time.Duration) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "ok") })
	mux.HandleFunc("POST "+Path, func(w http.ResponseWriter, r *http.Request) {
		body, urgent, ok := readRequest(w, r)
		if !ok {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		if err := q.Acquire(ctx, urgent); err != nil {
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
			sendError(w, ctx, err)
			return
		}
		defer res.Body.Close()
		if !passResponse(w, res) {
			return
		}
		if relay(w, res, role, nil) != relayTruncated {
			return
		}
		if r.Context().Err() == nil {
			log.Printf("worker stream ended before complete DONE event")
		}
		// Headers may already have reached the client. Abort the transport on truncation.
		panic(http.ErrAbortHandler)
	})
	return mux
}

// RoutedHandler serves the same API through a multi-worker Router. A request that fails before
// any byte reaches the client (connection error or HTTP 500/502/503/504) marks that worker down
// and is retried once on another eligible worker; a stream that has started is never retried.
func RoutedHandler(rt *Router, timeout time.Duration) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "ok") })
	mux.HandleFunc("GET /debug/workers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(rt.Status())
	})
	mux.HandleFunc("POST "+Path, func(w http.ResponseWriter, r *http.Request) {
		body, urgent, ok := readRequest(w, r)
		if !ok {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		acquireError := func(err error) {
			switch {
			case errors.Is(err, ErrFull):
				http.Error(w, err.Error(), 429)
			case errors.Is(err, ErrNoWorker):
				http.Error(w, err.Error(), 503)
			default:
				http.Error(w, "expired while queued", 504)
			}
		}
		lease, err := rt.Acquire(ctx, urgent, nil)
		if err != nil {
			acquireError(err)
			return
		}
		defer func() { lease.Release() }() // Holds the slot until the stream ends or the client is gone.
		failedBeforeStream := func(res *http.Response, err error) bool {
			if ctx.Err() != nil || (err == nil && !retryOnBackup(res.StatusCode)) {
				return false
			}
			why := "request failed before streaming"
			if err != nil {
				why += ": " + err.Error()
			} else {
				why += fmt.Sprintf(": HTTP %d", res.StatusCode)
			}
			rt.setHealthy(lease.t.w, false, why)
			return true
		}
		started := time.Now()
		res, err := send(ctx, lease.URL(), body)
		if failedBeforeStream(res, err) && rt.hasAlternative(lease.t.w) {
			if res != nil {
				res.Body.Close()
			}
			failed := lease.t.w
			lease.Release()
			if lease, err = rt.Acquire(ctx, urgent, failed); err != nil {
				acquireError(err)
				return
			}
			started = time.Now()
			res, err = send(ctx, lease.URL(), body)
			failedBeforeStream(res, err) // Marks the second worker down too; no further retry.
		}
		if err != nil {
			sendError(w, ctx, err)
			return
		}
		defer res.Body.Close()
		if !passResponse(w, res) {
			return
		}
		var firstToken time.Duration
		switch relay(w, res, lease.Name(), func() { firstToken = time.Since(started) }) {
		case relayDone:
			if firstToken > 0 {
				lease.Observe(firstToken, time.Since(started))
			}
		case relayTruncated:
			if r.Context().Err() == nil {
				log.Printf("worker %s stream ended before complete DONE event", lease.Name())
				if ctx.Err() == nil {
					rt.setHealthy(lease.t.w, false, "stream broke mid-response")
				}
			}
			panic(http.ErrAbortHandler)
		}
	})
	return mux
}
