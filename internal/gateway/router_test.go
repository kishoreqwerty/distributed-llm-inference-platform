package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeWorker streams a role event, optionally one content event, then waits for hold (if set)
// before finishing with content and [DONE]. GET requests are health probes.
type fakeWorker struct {
	srv                   *httptest.Server
	hold                  chan struct{}
	contentBeforeHold     bool
	calls, inflight, peak atomic.Int32
}

func newFake(t *testing.T, hold chan struct{}) *fakeWorker {
	t.Helper()
	f := &fakeWorker{hold: hold}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			return
		}
		f.calls.Add(1)
		n := f.inflight.Add(1)
		defer f.inflight.Add(-1)
		for p := f.peak.Load(); n > p && !f.peak.CompareAndSwap(p, n); p = f.peak.Load() {
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n")
		if f.contentBeforeHold {
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n")
		}
		w.(http.Flusher).Flush()
		if f.hold != nil {
			select {
			case <-f.hold:
			case <-r.Context().Done():
				return
			}
		}
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(func() {
		f.srv.CloseClientConnections()
		f.srv.Close()
	})
	return f
}

func est(first, full float64) Estimate { return Estimate{FirstTokenMs: first, FullMs: full} }

// macRog builds a two-worker config; estimates apply to both classes.
func macRog(mac, rog *fakeWorker, macSlots, rogSlots int, macEst, rogEst Estimate) RoutingConfig {
	return RoutingConfig{Workers: []WorkerConfig{
		{Name: "mac", URL: mac.srv.URL, Slots: macSlots, Urgent: macEst, Background: macEst},
		{Name: "rog", URL: rog.srv.URL, Slots: rogSlots, Urgent: rogEst, Background: rogEst},
	}}
}

func routed(t *testing.T, mode Mode, cfg RoutingConfig, limit int, priority bool) (*Router, string) {
	t.Helper()
	rt, err := NewRouter(mode, cfg, limit, priority)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Start())
	server := httptest.NewServer(RoutedHandler(rt, 5*time.Second))
	t.Cleanup(server.Close)
	return rt, server.URL
}

type reply struct {
	worker string
	status int
	err    error
	at     time.Time
}

// call posts one request and reads the whole body. The reply arrives on the returned channel.
func call(ctx context.Context, base string, urgent bool) <-chan reply {
	out := make(chan reply, 1)
	go func() {
		res, err := post(ctx, base, "x", urgent)
		if err != nil {
			out <- reply{err: err, at: time.Now()}
			return
		}
		defer res.Body.Close()
		_, err = io.ReadAll(res.Body)
		out <- reply{res.Header.Get("X-Inference-Worker"), res.StatusCode, err, time.Now()}
	}()
	return out
}

func get(t *testing.T, c <-chan reply) reply {
	t.Helper()
	select {
	case r := <-c:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("request did not finish")
	}
	return reply{}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func waiting(rt *Router) int { s := rt.Status(); return s.WaitingUrgent + s.WaitingBackground }

func TestEstimateUsesElapsedTimeAndRequestsAhead(t *testing.T) {
	rt, err := NewRouter(ModeLoadAware, RoutingConfig{Workers: []WorkerConfig{
		{Name: "mac", URL: "http://127.0.0.1:1", Slots: 1, Urgent: est(10, 100), Background: est(10, 100)},
		{Name: "rog", URL: "http://127.0.0.1:2", Slots: 1, Urgent: est(150, 300), Background: est(150, 300)},
	}}, 10, true)
	if err != nil {
		t.Fatal(err)
	}
	mac := rt.workers[0]
	a := &ticket{class: classUrgent, start: time.Now().Add(-60 * time.Millisecond)}
	if r := remainingMs(mac, a, time.Now()); r < 35 || r > 41 {
		t.Fatalf("100 ms estimate after 60 ms should leave ~40 ms, got %.1f", r)
	}
	a.start = time.Now().Add(-180 * time.Millisecond)
	if r := remainingMs(mac, a, time.Now()); r < 79 || r > 85 {
		t.Fatalf("an 80 ms overrun should predict ~80 ms more, got %.1f", r)
	}
	// A has run 20 ms of its 100 ms on the Mac. B: ~80 ms wait + 10 = ~90 ms on the Mac, < 150 on the
	// idle rog, so B waits. C is behind B: ~80 + 100 (B's turn) + 10 = ~190 ms > 150, so C goes to rog.
	a.start = time.Now().Add(-20 * time.Millisecond)
	rt.mu.Lock()
	mac.active[a] = struct{}{}
	a.w = mac
	b := &ticket{class: classUrgent, ready: make(chan struct{}), lane: &rt.urgent}
	b.elem = rt.urgent.PushBack(b)
	c := &ticket{class: classUrgent, ready: make(chan struct{}), lane: &rt.urgent}
	c.elem = rt.urgent.PushBack(c)
	rt.scheduleLocked()
	rt.mu.Unlock()
	if b.w != nil || c.w == nil || c.w.cfg.Name != "rog" {
		t.Fatalf("want B waiting for the Mac and C sent to rog; got B=%v C=%v", b.w, c.w)
	}
}

func TestLoadAwareShortMacWaitStaysOnMac(t *testing.T) {
	hold := make(chan struct{})
	mac, rog := newFake(t, hold), newFake(t, nil)
	// Busy Mac: ~150 ms left + 10 ms first token beats 200 ms on the idle rog.
	rt, base := routed(t, ModeLoadAware, macRog(mac, rog, 1, 1, est(10, 150), est(200, 300)), 8, true)
	ctx := context.Background()
	first := call(ctx, base, true)
	waitFor(t, "first request on the Mac", func() bool { return mac.inflight.Load() == 1 })
	second := call(ctx, base, true)
	waitFor(t, "second request queued", func() bool { return waiting(rt) == 1 })
	time.Sleep(30 * time.Millisecond)
	if rog.calls.Load() != 0 || waiting(rt) != 1 {
		t.Fatal("request left the queue although the Mac wait was short")
	}
	close(hold)
	if r := get(t, first); r.worker != "mac" || r.status != 200 {
		t.Fatalf("first: %+v", r)
	}
	if r := get(t, second); r.worker != "mac" || r.status != 200 || r.err != nil {
		t.Fatalf("second should wait for the Mac: %+v", r)
	}
	if rog.calls.Load() != 0 {
		t.Fatal("rog was used")
	}
}

func TestLoadAwareLongMacWaitGoesToRog(t *testing.T) {
	hold := make(chan struct{})
	defer close(hold)
	mac, rog := newFake(t, hold), newFake(t, nil)
	// Busy Mac: ~2 s left, so the idle rog (200 ms) is faster now.
	_, base := routed(t, ModeLoadAware, macRog(mac, rog, 1, 1, est(10, 2000), est(200, 300)), 8, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	call(ctx, base, true)
	waitFor(t, "first request on the Mac", func() bool { return mac.inflight.Load() == 1 })
	start := time.Now()
	if r := get(t, call(ctx, base, true)); r.worker != "rog" || r.status != 200 {
		t.Fatalf("want rog, got %+v", r)
	}
	if time.Since(start) > time.Second {
		t.Fatal("request waited instead of going to rog")
	}
}

func TestLoadAwareReconsidersWhenMacOverruns(t *testing.T) {
	hold := make(chan struct{})
	defer close(hold)
	mac, rog := newFake(t, hold), newFake(t, nil)
	// Mac expected free in ~50 ms (+10 first token < 100 on rog), but its request never finishes.
	// Once the overrun makes the Mac look slower than rog (~140 ms in), the queued request moves.
	_, base := routed(t, ModeLoadAware, macRog(mac, rog, 1, 1, est(10, 50), est(100, 200)), 8, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	call(ctx, base, true)
	waitFor(t, "first request on the Mac", func() bool { return mac.inflight.Load() == 1 })
	start := time.Now()
	r := get(t, call(ctx, base, true))
	if r.worker != "rog" || r.status != 200 {
		t.Fatalf("want rog after the Mac overran, got %+v", r)
	}
	if waited := time.Since(start); waited < 60*time.Millisecond || waited > 2*time.Second {
		t.Fatalf("expected a bounded wait before moving to rog, waited %v", waited)
	}
}

func TestPerWorkerSlotLimits(t *testing.T) {
	for _, mode := range []Mode{ModeRoundRobin, ModeLoadAware} {
		t.Run(string(mode), func(t *testing.T) {
			hold := make(chan struct{})
			mac, rog := newFake(t, hold), newFake(t, nil)
			rog.hold = hold
			// Equal estimates so load-aware also fills both workers.
			rt, base := routed(t, mode, macRog(mac, rog, 2, 1, est(10, 20), est(10, 20)), 8, true)
			ctx := context.Background()
			var replies []<-chan reply
			for i := 0; i < 5; i++ {
				replies = append(replies, call(ctx, base, i%2 == 0))
			}
			waitFor(t, "2 on mac, 1 on rog, 2 waiting", func() bool {
				return mac.inflight.Load() == 2 && rog.inflight.Load() == 1 && waiting(rt) == 2
			})
			time.Sleep(50 * time.Millisecond)
			if mac.inflight.Load() != 2 || rog.inflight.Load() != 1 {
				t.Fatal("slot limit exceeded while requests were waiting")
			}
			close(hold)
			for _, c := range replies {
				if r := get(t, c); r.status != 200 || r.err != nil {
					t.Fatalf("%+v", r)
				}
			}
			if mac.peak.Load() != 2 || rog.peak.Load() != 1 || mac.calls.Load()+rog.calls.Load() != 5 {
				t.Fatalf("peaks mac=%d rog=%d calls=%d", mac.peak.Load(), rog.peak.Load(), mac.calls.Load()+rog.calls.Load())
			}
			s := rt.Status()
			if s.Workers[0].Active != 0 || s.Workers[1].Active != 0 {
				t.Fatalf("slots not released: %+v", s)
			}
		})
	}
}

func TestLoadAwareBurstRespectsSlots(t *testing.T) {
	mac, rog := newFake(t, nil), newFake(t, nil)
	_, base := routed(t, ModeLoadAware, macRog(mac, rog, 4, 1, est(1, 2), est(5, 10)), 200, true)
	var wg sync.WaitGroup
	var bad atomic.Int32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r := <-call(context.Background(), base, i%4 == 2); r.status != 200 || r.err != nil {
				bad.Add(1)
			}
		}()
	}
	wg.Wait()
	if bad.Load() != 0 || mac.peak.Load() > 4 || rog.peak.Load() > 1 {
		t.Fatalf("failures=%d peak mac=%d rog=%d", bad.Load(), mac.peak.Load(), rog.peak.Load())
	}
}

func TestRoundRobinAlternates(t *testing.T) {
	mac, rog := newFake(t, nil), newFake(t, nil)
	_, base := routed(t, ModeRoundRobin, macRog(mac, rog, 4, 1, est(30, 40), est(170, 270)), 8, true)
	var got []string
	for i := 0; i < 4; i++ {
		got = append(got, get(t, call(context.Background(), base, false)).worker)
	}
	if strings.Join(got, ",") != "mac,rog,mac,rog" {
		t.Fatalf("got %v", got)
	}
}

func TestPrimaryOnlyNeverUsesOthers(t *testing.T) {
	mac, rog := newFake(t, nil), newFake(t, nil)
	_, base := routed(t, ModePrimaryOnly, macRog(mac, rog, 1, 1, est(500, 1000), est(1, 2)), 8, true)
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r := <-call(context.Background(), base, false); r.worker != "mac" || r.status != 200 {
				t.Errorf("%+v", r)
			}
		}()
	}
	wg.Wait()
	if rog.calls.Load() != 0 || mac.peak.Load() != 1 {
		t.Fatalf("rog calls=%d mac peak=%d", rog.calls.Load(), mac.peak.Load())
	}
	mac.srv.Listener.Close() // Mac gone: no alternative in primary-only, so the error passes through.
	mac.srv.CloseClientConnections()
	if r := get(t, call(context.Background(), base, false)); r.status != 502 || rog.calls.Load() != 0 {
		t.Fatalf("want 502 without using rog, got %+v", r)
	}
}

func TestRogLossBeforeStreamRetriesOnMac(t *testing.T) {
	mac, rog := newFake(t, nil), newFake(t, nil)
	rt, err := NewRouter(ModeRoundRobin, macRog(mac, rog, 4, 1, est(30, 40), est(170, 270)), 8, true)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(RoutedHandler(rt, 5*time.Second)) // No health probes: passive detection only.
	defer server.Close()
	rog.srv.Listener.Close()
	for i := 0; i < 4; i++ {
		if r := get(t, call(context.Background(), server.URL, false)); r.worker != "mac" || r.status != 200 || r.err != nil {
			t.Fatalf("request %d: %+v", i, r)
		}
	}
	if s := rt.Status(); s.Workers[1].Healthy || s.Workers[1].Dispatched != 1 || mac.calls.Load() != 4 {
		t.Fatalf("rog should be marked down after one refused request: %+v mac=%d", s, mac.calls.Load())
	}
}

func TestRogLossReplansQueuedAndNeverRetriesStartedStream(t *testing.T) {
	hold := make(chan struct{})
	defer close(hold)
	mac, rog := newFake(t, nil), newFake(t, hold)
	rog.contentBeforeHold = true
	// Here rog is the fast worker, so a request queues for it rather than use the idle Mac.
	cfg := macRog(mac, rog, 4, 1, est(1000, 1200), est(10, 500)) // rog: ~500+10 ms < 1000 on the Mac, until ~1.5 s overrun
	cfg.HealthIntervalMs = 20
	cfg.HealthPath = "/health"
	rt, base := routed(t, ModeLoadAware, cfg, 8, true)
	ctx := context.Background()
	streaming := call(ctx, base, true)
	waitFor(t, "stream started on rog", func() bool { return rog.inflight.Load() == 1 })
	queuedForRog := call(ctx, base, true)
	waitFor(t, "request queued for rog", func() bool { return waiting(rt) == 1 })
	rog.srv.Listener.Close()
	rog.srv.CloseClientConnections()
	r := get(t, streaming)
	if r.worker != "rog" || r.err == nil {
		t.Fatalf("a started stream must break, not be retried: %+v", r)
	}
	if r := get(t, queuedForRog); r.worker != "mac" || r.status != 200 || r.err != nil {
		t.Fatalf("queued request should move to the Mac: %+v", r)
	}
	if rog.calls.Load() != 1 || mac.calls.Load() != 1 {
		t.Fatalf("calls rog=%d mac=%d, want 1 and 1", rog.calls.Load(), mac.calls.Load())
	}
	waitFor(t, "rog marked down", func() bool { return !rt.Status().Workers[1].Healthy })
}

func TestAllWorkersLostFailsQueuedRequests(t *testing.T) {
	hold := make(chan struct{})
	defer close(hold)
	mac := newFake(t, hold)
	mac.contentBeforeHold = true
	rt, err := NewRouter(ModePrimaryOnly, RoutingConfig{Workers: []WorkerConfig{
		{Name: "mac", URL: mac.srv.URL, Slots: 1, Urgent: est(10, 20), Background: est(10, 20)}}}, 8, true)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(RoutedHandler(rt, 5*time.Second))
	t.Cleanup(server.Close) // After close(hold), so the held request can finish.
	call(context.Background(), server.URL, false)
	waitFor(t, "first on mac", func() bool { return mac.inflight.Load() == 1 })
	queued := call(context.Background(), server.URL, false)
	waitFor(t, "second queued", func() bool { return waiting(rt) == 1 })
	rt.setHealthy(rt.workers[0], false, "test")
	if r := get(t, queued); r.status != 503 {
		t.Fatalf("queued request should fail fast with 503, got %+v", r)
	}
	if r := get(t, call(context.Background(), server.URL, false)); r.status != 503 {
		t.Fatalf("new request should get 503, got %+v", r)
	}
}

func TestRoutedPriorityAndQueueLimit(t *testing.T) {
	hold := make(chan struct{})
	mac := newFake(t, hold)
	cfg := RoutingConfig{Workers: []WorkerConfig{{Name: "mac", URL: mac.srv.URL, Slots: 1, Urgent: est(10, 20), Background: est(10, 20)}}}
	for _, mode := range []Mode{ModePrimaryOnly, ModeLoadAware} {
		rt, base := routed(t, mode, cfg, 2, true)
		ctx := context.Background()
		first := call(ctx, base, false)
		waitFor(t, "first on mac", func() bool { return mac.inflight.Load() == 1 })
		background := call(ctx, base, false)
		waitFor(t, "background queued", func() bool { return waiting(rt) == 1 })
		urgent := call(ctx, base, true)
		waitFor(t, "urgent queued", func() bool { return waiting(rt) == 2 })
		if r := get(t, call(ctx, base, true)); r.status != 429 {
			t.Fatalf("%s: overflow got %+v", mode, r)
		}
		hold <- struct{}{}
		get(t, first)
		hold <- struct{}{}
		u := get(t, urgent)
		hold <- struct{}{}
		b := get(t, background)
		if u.status != 200 || b.status != 200 || !u.at.Before(b.at) {
			t.Fatalf("%s: urgent should finish before background: %+v %+v", mode, u, b)
		}
	}
}

func TestRoutedClientDisconnectReleasesSlot(t *testing.T) {
	hold := make(chan struct{})
	defer close(hold)
	mac := newFake(t, hold)
	mac.contentBeforeHold = true
	rt, base := routed(t, ModeLoadAware, RoutingConfig{Workers: []WorkerConfig{
		{Name: "mac", URL: mac.srv.URL, Slots: 1, Urgent: est(10, 20), Background: est(10, 20)}}}, 8, true)
	ctx, cancel := context.WithCancel(context.Background())
	res, err := post(ctx, base, "x", false)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	buf := make([]byte, 16)
	if _, err := res.Body.Read(buf); err != nil {
		t.Fatal(err)
	}
	if rt.Status().Workers[0].Active != 1 {
		t.Fatal("slot should be held while streaming")
	}
	cancel()
	waitFor(t, "slot released after disconnect", func() bool { return rt.Status().Workers[0].Active == 0 })
	if !rt.Status().Workers[0].Healthy {
		t.Fatal("a client disconnect must not mark the worker down")
	}
}

func TestEWMAUpdatesEstimates(t *testing.T) {
	mac := newFake(t, nil)
	cfg := RoutingConfig{EWMAAlpha: 0.5, Workers: []WorkerConfig{
		{Name: "mac", URL: mac.srv.URL, Slots: 1, Urgent: est(1000, 2000), Background: est(1000, 2000)}}}
	rt, base := routed(t, ModeLoadAware, cfg, 8, true)
	get(t, call(context.Background(), base, true))
	s := rt.Status().Workers[0]
	if s.Urgent.FirstTokenMs > 600 || s.Urgent.FullMs > 1100 || s.Background.FirstTokenMs != 1000 {
		t.Fatalf("urgent estimate should move halfway toward ~0 ms, background unchanged: %+v", s)
	}
}

func TestRoutingConfig(t *testing.T) {
	cfg, err := LoadRoutingConfig("../../configs/phase3-mac-rog.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Workers) != 2 || cfg.Workers[0].Name != "mac" || cfg.Workers[0].Slots != 4 || cfg.Workers[1].Slots != 1 ||
		cfg.Workers[1].Background != est(177.7, 476.1) {
		t.Fatalf("unexpected benchmark config: %+v", cfg)
	}
	good := WorkerConfig{Name: "a", URL: "http://127.0.0.1:1", Slots: 1, Urgent: est(1, 2), Background: est(1, 2)}
	for name, mutate := range map[string]func(*RoutingConfig){
		"no workers":     func(c *RoutingConfig) { c.Workers = nil },
		"duplicate name": func(c *RoutingConfig) { c.Workers = append(c.Workers, good) },
		"bad name":       func(c *RoutingConfig) { c.Workers[0].Name = "a b" },
		"bad url":        func(c *RoutingConfig) { c.Workers[0].URL = "127.0.0.1:1" },
		"zero slots":     func(c *RoutingConfig) { c.Workers[0].Slots = 0 },
		"full < first":   func(c *RoutingConfig) { c.Workers[0].Urgent = est(5, 2) },
		"zero estimate":  func(c *RoutingConfig) { c.Workers[0].Background = est(0, 2) },
		"alpha":          func(c *RoutingConfig) { c.EWMAAlpha = 2 },
		"interval":       func(c *RoutingConfig) { c.HealthIntervalMs = 5 },
	} {
		c := RoutingConfig{HealthPath: "/health", Workers: []WorkerConfig{good}}
		mutate(&c)
		if c.Validate() == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := ParseMode("mac-only"); err == nil {
		t.Error("unknown mode accepted")
	}
}
