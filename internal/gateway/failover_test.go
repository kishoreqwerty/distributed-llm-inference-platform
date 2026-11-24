package gateway

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPrimaryBackupRecovery(t *testing.T) {
	var status atomic.Int32
	var calls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(int(status.Load()))
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer primary.Close()
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer backup.Close()
	handler := HandlerWithBackup(primary.URL, backup.URL, NewQueue(1, 2, true), time.Second)
	for _, c := range []struct {
		status   int
		role     string
		expected int
	}{{200, "primary", 200}, {503, "backup", 200}, {400, "", 400}, {200, "primary", 200}} {
		status.Store(int32(c.status))
		r := httptest.NewRequest("POST", Path, strings.NewReader("{\"stream\":true}"))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != c.expected || w.Header().Get("X-Inference-Worker") != c.role {
			t.Fatalf("status %d: got %d %v", c.status, w.Code, w.Header())
		}
	}
	primary.Close()
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("POST", Path, strings.NewReader("{\"stream\":true}")))
	if w.Code != 200 || w.Header().Get("X-Inference-Worker") != "backup" || calls.Load() != 2 {
		t.Fatal("connection refusal did not fail over", w.Code, calls.Load())
	}
}

// countingWorker answers every request with status; 200 responses stream one content event and [DONE].
func countingWorker(t *testing.T, status int, calls *atomic.Int32) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if status != 200 {
			http.Error(w, "engine shutting down", status)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(s.Close)
	return s
}

func TestPrimary500FailsOverToBackup(t *testing.T) {
	var primaryCalls, backupCalls atomic.Int32
	primary := countingWorker(t, 500, &primaryCalls)
	backup := countingWorker(t, 200, &backupCalls)
	handler := HandlerWithBackup(primary.URL, backup.URL, NewQueue(1, 2, true), time.Second)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("POST", Path, strings.NewReader(`{"stream":true}`)))
	if w.Code != 200 || w.Header().Get("X-Inference-Worker") != "backup" {
		t.Fatalf("got %d worker=%q body=%q", w.Code, w.Header().Get("X-Inference-Worker"), w.Body.String())
	}
	if body := w.Body.String(); !strings.Contains(body, `"content":"hi"`) || !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("backup stream not relayed: %q", body)
	}
	if primaryCalls.Load() != 1 || backupCalls.Load() != 1 {
		t.Fatalf("calls primary=%d backup=%d, want 1 and 1", primaryCalls.Load(), backupCalls.Load())
	}
}

func TestBackup500IsNotRetried(t *testing.T) {
	var primaryCalls, backupCalls atomic.Int32
	primary := countingWorker(t, 500, &primaryCalls)
	backup := countingWorker(t, 500, &backupCalls)
	handler := HandlerWithBackup(primary.URL, backup.URL, NewQueue(1, 2, true), time.Second)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("POST", Path, strings.NewReader(`{"stream":true}`)))
	if w.Code != 500 {
		t.Fatalf("got %d body=%q, want the backup's 500", w.Code, w.Body.String())
	}
	if primaryCalls.Load() != 1 || backupCalls.Load() != 1 {
		t.Fatalf("calls primary=%d backup=%d, want 1 and 1", primaryCalls.Load(), backupCalls.Load())
	}
}

func TestPrimary500WithoutBackupPassesThrough(t *testing.T) {
	var calls atomic.Int32
	primary := countingWorker(t, 500, &calls)
	w := httptest.NewRecorder()
	HandlerWithBackup(primary.URL, "", NewQueue(1, 2, true), time.Second).
		ServeHTTP(w, httptest.NewRequest("POST", Path, strings.NewReader(`{"stream":true}`)))
	if w.Code != 500 || calls.Load() != 1 {
		t.Fatalf("got %d after %d calls, want 500 after 1", w.Code, calls.Load())
	}
}
