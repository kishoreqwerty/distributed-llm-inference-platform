package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func queued(t *testing.T, q *Queue, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		q.mu.Lock()
		count := q.urgent.Len() + q.normal.Len()
		q.mu.Unlock()
		if count == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("never observed %d waiting requests", n)
}

func post(ctx context.Context, base, label string, urgent bool) (*http.Response, error) {
	body := fmt.Sprintf("{\"stream\":true,\"label\":%q}", label)
	req, err := http.NewRequestWithContext(ctx, "POST", base+Path, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	if urgent {
		req.Header.Set("X-Request-Priority", "urgent")
	}
	return http.DefaultClient.Do(req)
}

func TestAdmissionOrder(t *testing.T) {
	for _, priority := range []bool{false, true} {
		t.Run(fmt.Sprintf("priority=%v", priority), func(t *testing.T) {
			started := make(chan string, 3)
			advance := make(chan struct{}, 3)
			worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				started <- string(body)
				select {
				case <-advance:
				case <-r.Context().Done():
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, "data: [DONE]\n\n")
			}))
			defer worker.Close()
			q := NewQueue(1, 2, priority)
			server := httptest.NewServer(Handler(worker.URL, q, 5*time.Second))
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			completed := make(chan error, 3)
			launch := func(label string, urgent bool) {
				go func() {
					res, err := post(ctx, server.URL, label, urgent)
					if err == nil {
						_, err = io.Copy(io.Discard, res.Body)
						res.Body.Close()
						if res.StatusCode != 200 {
							err = fmt.Errorf("status %d", res.StatusCode)
						}
					}
					completed <- err
				}()
			}
			expect := func(label string) {
				t.Helper()
				select {
				case body := <-started:
					if !strings.Contains(body, label) {
						t.Fatalf("expected %s; got %s", label, body)
					}
				case <-time.After(3 * time.Second):
					t.Fatalf("timeout waiting for %s", label)
				}
			}
			launch("B1", false)
			expect("B1")
			launch("B2", false)
			queued(t, q, 1)
			launch("U1", true)
			queued(t, q, 2)
			res, err := post(ctx, server.URL, "overflow", true)
			if err != nil {
				t.Fatal(err)
			}
			res.Body.Close()
			if res.StatusCode != 429 {
				t.Fatalf("overflow status %d", res.StatusCode)
			}
			next, last := "B2", "U1"
			if priority {
				next, last = last, next
			}
			advance <- struct{}{}
			expect(next)
			advance <- struct{}{}
			expect(last)
			advance <- struct{}{}
			for range 3 {
				if err := <-completed; err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestStreamAndCancellation(t *testing.T) {
	ended := make(chan struct{})
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(ended)
	}))
	defer worker.Close()
	q := NewQueue(1, 1, false)
	server := httptest.NewServer(Handler(worker.URL, q, 5*time.Second))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res, err := post(ctx, server.URL, "one", false)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	buf := make([]byte, len("data: first\n\n"))
	if _, err := io.ReadFull(res.Body, buf); err != nil || string(buf) != "data: first\n\n" {
		t.Fatal("first event did not stream", err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	done := make(chan error, 1)
	go func() {
		response, err := post(ctx2, server.URL, "two", false)
		if response != nil {
			response.Body.Close()
		}
		done <- err
	}()
	queued(t, q, 1)
	cancel2()
	<-done
	queued(t, q, 0)
	cancel()
	select {
	case <-ended:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not see cancellation")
	}
	acquire, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err = q.Acquire(acquire, false); err != nil {
		t.Fatal(err)
	}
	q.Release()
}

func TestTruncatedUpstream(t *testing.T) {
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: partial\n\n")
	}))
	defer worker.Close()
	server := httptest.NewServer(Handler(worker.URL, NewQueue(1, 1, false), time.Second))
	defer server.Close()
	res, err := post(context.Background(), server.URL, "bad", false)
	if err == nil {
		defer res.Body.Close()
		_, err = io.ReadAll(res.Body)
	}
	if err == nil {
		t.Fatal("expected a transport error on premature EOF")
	}
}
