package gateway

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

// ErrNoWorker means no healthy worker is eligible for the request in the current routing mode.
var ErrNoWorker = errors.New("no healthy worker available")

type Mode string

const (
	ModePrimaryOnly Mode = "primary-only" // Only the first configured worker.
	ModeRoundRobin  Mode = "round-robin"  // Rotate across healthy workers that have a free slot.
	ModeLoadAware   Mode = "load-aware"   // Lowest estimated time to first token, possibly after a wait.
)

func ParseMode(s string) (Mode, error) {
	switch m := Mode(s); m {
	case ModePrimaryOnly, ModeRoundRobin, ModeLoadAware:
		return m, nil
	}
	return "", fmt.Errorf("unknown routing mode %q (want primary-only, round-robin or load-aware)", s)
}

// Estimate is a worker's expected service time for one request class, measured from dispatch.
type Estimate struct {
	FirstTokenMs float64 `json:"first_token_ms"`
	FullMs       float64 `json:"full_ms"`
}

type WorkerConfig struct {
	Name       string   `json:"name"`
	URL        string   `json:"url"`
	Slots      int      `json:"slots"`
	Urgent     Estimate `json:"urgent"`
	Background Estimate `json:"background"`
}

// RoutingConfig holds the hardware-specific values. It lives in benchmark configuration, not code.
type RoutingConfig struct {
	Workers          []WorkerConfig `json:"workers"`
	HealthPath       string         `json:"health_path"`        // Default /health. Any status below 500 is healthy.
	HealthIntervalMs int            `json:"health_interval_ms"` // Default 500. 0 disables active probes.
	EWMAAlpha        float64        `json:"ewma_alpha"`         // Weight of each completed request in the estimates; 0 keeps them fixed.
}

var workerName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,32}$`)

func LoadRoutingConfig(path string) (RoutingConfig, error) {
	var cfg RoutingConfig
	f, err := os.Open(path)
	if err != nil {
		return cfg, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err = dec.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("%s: %w", path, err)
	}
	if cfg.HealthPath == "" {
		cfg.HealthPath = "/health"
	}
	return cfg, cfg.Validate()
}

func (cfg RoutingConfig) Validate() error {
	if len(cfg.Workers) == 0 {
		return errors.New("routing config needs at least one worker")
	}
	seen := map[string]bool{}
	for _, w := range cfg.Workers {
		if !workerName.MatchString(w.Name) || seen[w.Name] {
			return fmt.Errorf("worker name %q must be unique and match %s", w.Name, workerName)
		}
		seen[w.Name] = true
		u, err := url.Parse(w.URL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("worker %s: invalid base URL", w.Name)
		}
		if w.Slots < 1 {
			return fmt.Errorf("worker %s: slots must be at least 1", w.Name)
		}
		for _, e := range []Estimate{w.Urgent, w.Background} {
			if !(e.FirstTokenMs > 0) || !(e.FullMs >= e.FirstTokenMs) || math.IsInf(e.FullMs, 0) {
				return fmt.Errorf("worker %s: estimates need 0 < first_token_ms <= full_ms", w.Name)
			}
		}
	}
	if !strings.HasPrefix(cfg.HealthPath, "/") {
		return errors.New("health_path must start with /")
	}
	if cfg.HealthIntervalMs != 0 && (cfg.HealthIntervalMs < 20 || cfg.HealthIntervalMs > 60000) {
		return errors.New("health_interval_ms must be 0 or 20..60000")
	}
	if !(cfg.EWMAAlpha >= 0 && cfg.EWMAAlpha <= 1) {
		return errors.New("ewma_alpha must be in [0, 1]")
	}
	return nil
}

const (
	classUrgent = iota
	classBackground
)

// While a load-aware request is deliberately waiting although some slot is free, the plan is
// recomputed this often, because elapsed time alone can change which worker is faster.
const replanEvery = 5 * time.Millisecond

type rworker struct {
	cfg        WorkerConfig
	est        [2]Estimate
	healthy    bool
	active     map[*ticket]struct{}
	dispatched int
}

type ticket struct {
	class   int
	exclude *rworker // Worker that already failed this request (retry).
	lane    *list.List
	elem    *list.Element
	ready   chan struct{} // Closed once w or err is set.
	w       *rworker
	err     error
	start   time.Time
}

type Router struct {
	mu          sync.Mutex
	mode        Mode
	workers     []*rworker
	urgent      list.List
	normal      list.List
	limit       int
	priority    bool
	rr          int
	timerArmed  bool
	alpha       float64
	healthPath  string
	healthEvery time.Duration
}

// NewRouter routes across cfg.Workers. limit is the maximum number of waiting requests (shared by
// all workers); priority admits urgent requests before background ones, as the single-worker Queue does.
func NewRouter(mode Mode, cfg RoutingConfig, limit int, priority bool) (*Router, error) {
	if _, err := ParseMode(string(mode)); err != nil {
		return nil, err
	}
	if cfg.HealthPath == "" {
		cfg.HealthPath = "/health"
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, errors.New("negative queue limit")
	}
	rt := &Router{mode: mode, limit: limit, priority: priority, alpha: cfg.EWMAAlpha, healthPath: cfg.HealthPath,
		healthEvery: time.Duration(cfg.HealthIntervalMs) * time.Millisecond}
	for _, c := range cfg.Workers {
		rt.workers = append(rt.workers, &rworker{cfg: c, est: [2]Estimate{c.Urgent, c.Background},
			healthy: true, active: map[*ticket]struct{}{}})
	}
	return rt, nil
}

// Lease is one held worker slot. Release it exactly when the stream ends or the client is gone.
type Lease struct {
	rt       *Router
	t        *ticket
	released bool
}

func (l *Lease) Name() string { return l.t.w.cfg.Name }
func (l *Lease) URL() string  { return l.t.w.cfg.URL }

func (l *Lease) Release() {
	if l == nil {
		return
	}
	l.rt.mu.Lock()
	defer l.rt.mu.Unlock()
	if l.released {
		return
	}
	l.released = true
	delete(l.t.w.active, l.t)
	l.rt.scheduleLocked()
}

// Observe folds one successful request's measured times into the worker's estimates.
func (l *Lease) Observe(firstToken, full time.Duration) {
	rt := l.rt
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.alpha == 0 {
		return
	}
	e := &l.t.w.est[l.t.class]
	e.FirstTokenMs = (1-rt.alpha)*e.FirstTokenMs + rt.alpha*ms(firstToken)
	e.FullMs = math.Max((1-rt.alpha)*e.FullMs+rt.alpha*ms(full), e.FirstTokenMs)
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func (rt *Router) eligibleLocked(i int, t *ticket) bool {
	w := rt.workers[i]
	return w.healthy && w != t.exclude && (rt.mode != ModePrimaryOnly || i == 0)
}

func (rt *Router) anyEligibleLocked(t *ticket) bool {
	for i := range rt.workers {
		if rt.eligibleLocked(i, t) {
			return true
		}
	}
	return false
}

func (rt *Router) waitingLocked() int { return rt.urgent.Len() + rt.normal.Len() }

// Acquire waits for a worker slot chosen by the routing mode. exclude is the worker that just
// failed this request before streaming; such a retry goes to the front of its lane and skips the
// queue limit, because the request was already admitted.
func (rt *Router) Acquire(ctx context.Context, urgent bool, exclude *rworker) (*Lease, error) {
	rt.mu.Lock()
	if err := ctx.Err(); err != nil {
		rt.mu.Unlock()
		return nil, err
	}
	t := &ticket{class: classBackground, exclude: exclude, ready: make(chan struct{})}
	if urgent {
		t.class = classUrgent
	}
	if !rt.anyEligibleLocked(t) {
		rt.mu.Unlock()
		return nil, ErrNoWorker
	}
	t.lane = &rt.normal
	if rt.priority && urgent {
		t.lane = &rt.urgent
	}
	if exclude != nil {
		t.elem = t.lane.PushFront(t)
	} else {
		t.elem = t.lane.PushBack(t)
	}
	rt.scheduleLocked()
	if t.w == nil && t.err == nil && exclude == nil && rt.waitingLocked()-1 >= rt.limit {
		t.lane.Remove(t.elem)
		rt.scheduleLocked()
		rt.mu.Unlock()
		return nil, ErrFull
	}
	rt.mu.Unlock()
	select {
	case <-t.ready:
	case <-ctx.Done():
		rt.mu.Lock()
		defer rt.mu.Unlock()
		select {
		case <-t.ready:
			if t.w != nil { // A slot was assigned while cancellation was pending.
				delete(t.w.active, t)
			}
		default:
			t.lane.Remove(t.elem)
		}
		rt.scheduleLocked()
		return nil, ctx.Err()
	}
	if t.err != nil {
		return nil, t.err
	}
	return &Lease{rt: rt, t: t}, nil
}

// remainingMs estimates how long an active request keeps its slot: its full-request estimate minus
// the time it has run. A request that has overrun its estimate is assumed to need as long again as
// it has overrun, so a stuck worker looks slower the longer it stays stuck.
func remainingMs(w *rworker, a *ticket, now time.Time) float64 {
	return math.Abs(w.est[a.class].FullMs - ms(now.Sub(a.start)))
}

type projSlot struct {
	at   float64 // ms from now until the slot is expected to be free for the next request.
	free bool    // Free right now.
}

// scheduleLocked assigns waiting requests in admission order (urgent lane first under priority).
func (rt *Router) scheduleLocked() {
	now := time.Now()
	n := len(rt.workers)
	free := make([]int, n)
	var proj [][]projSlot
	if rt.mode == ModeLoadAware {
		proj = make([][]projSlot, n)
	}
	for i, w := range rt.workers {
		free[i] = w.cfg.Slots - len(w.active)
		if proj == nil {
			continue
		}
		for k := 0; k < free[i]; k++ {
			proj[i] = append(proj[i], projSlot{0, true})
		}
		for a := range w.active {
			proj[i] = append(proj[i], projSlot{remainingMs(w, a, now), false})
		}
	}
	waiting := 0
	for _, lane := range []*list.List{&rt.urgent, &rt.normal} {
		for e := lane.Front(); e != nil; {
			next := e.Next()
			t := e.Value.(*ticket)
			choice, eligible := -1, false
			if rt.mode == ModeLoadAware {
				best, bestK, bestAt := -1, -1, math.Inf(1)
				for i := 0; i < n; i++ {
					if !rt.eligibleLocked(i, t) || len(proj[i]) == 0 {
						continue
					}
					eligible = true
					k := 0
					for j, s := range proj[i] {
						if s.at < proj[i][k].at || (s.at == proj[i][k].at && s.free && !proj[i][k].free) {
							k = j
						}
					}
					if at := proj[i][k].at + rt.workers[i].est[t.class].FirstTokenMs; at < bestAt {
						best, bestK, bestAt = i, k, at
					}
				}
				if best >= 0 {
					s := &proj[best][bestK]
					full := rt.workers[best].est[t.class].FullMs
					if s.free {
						choice, *s = best, projSlot{full, false}
					} else {
						s.at += full // Planned: it holds this slot after the current occupant.
					}
				}
			} else {
				for k := 0; k < n; k++ {
					i := (rt.rr + k) % n
					if !rt.eligibleLocked(i, t) {
						continue
					}
					eligible = true
					if free[i] > 0 {
						choice = i
						break
					}
				}
				if choice >= 0 {
					rt.rr = (choice + 1) % n
				}
			}
			switch {
			case !eligible:
				lane.Remove(e)
				t.err = ErrNoWorker
				close(t.ready)
			case choice >= 0:
				lane.Remove(e)
				w := rt.workers[choice]
				t.w, t.start = w, now
				w.active[t] = struct{}{}
				w.dispatched++
				free[choice]--
				close(t.ready)
			default:
				waiting++
			}
			e = next
		}
	}
	if rt.mode != ModeLoadAware || waiting == 0 || rt.timerArmed {
		return
	}
	for i, w := range rt.workers {
		if w.healthy && free[i] > 0 {
			rt.timerArmed = true
			time.AfterFunc(replanEvery, func() {
				rt.mu.Lock()
				defer rt.mu.Unlock()
				rt.timerArmed = false
				rt.scheduleLocked()
			})
			return
		}
	}
}

func (rt *Router) setHealthy(w *rworker, healthy bool, why string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if w.healthy == healthy {
		return
	}
	w.healthy = healthy
	state := "down"
	if healthy {
		state = "up"
	}
	log.Printf("worker %s %s: %s", w.cfg.Name, state, why)
	rt.scheduleLocked() // Requests planned for a lost worker are re-planned, or fail if none is left.
}

// hasAlternative reports whether a request that failed on w before streaming can be retried.
func (rt *Router) hasAlternative(w *rworker) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.anyEligibleLocked(&ticket{exclude: w})
}

// Start launches the active health probes and returns a function that stops them.
func (rt *Router) Start() (stop func()) {
	if rt.healthEvery <= 0 {
		return func() {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	client := &http.Client{Timeout: max(rt.healthEvery, 250*time.Millisecond)}
	for _, w := range rt.workers {
		go func() {
			tick := time.NewTicker(rt.healthEvery)
			defer tick.Stop()
			for {
				req, _ := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(w.cfg.URL, "/")+rt.healthPath, nil)
				res, err := client.Do(req)
				switch {
				case ctx.Err() != nil:
					return
				case err != nil:
					rt.setHealthy(w, false, "health probe: "+err.Error())
				default:
					io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
					res.Body.Close()
					rt.setHealthy(w, res.StatusCode < 500, fmt.Sprintf("health probe HTTP %d", res.StatusCode))
				}
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
			}
		}()
	}
	return cancel
}

type WorkerStatus struct {
	Name       string   `json:"name"`
	Healthy    bool     `json:"healthy"`
	Slots      int      `json:"slots"`
	Active     int      `json:"active"`
	Dispatched int      `json:"dispatched"`
	Urgent     Estimate `json:"urgent"`
	Background Estimate `json:"background"`
}

type RouterStatus struct {
	Mode              Mode           `json:"mode"`
	WaitingUrgent     int            `json:"waiting_urgent"`
	WaitingBackground int            `json:"waiting_background"`
	Workers           []WorkerStatus `json:"workers"`
}

func (rt *Router) Status() RouterStatus {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	s := RouterStatus{Mode: rt.mode}
	for _, lane := range []*list.List{&rt.urgent, &rt.normal} {
		for e := lane.Front(); e != nil; e = e.Next() {
			if e.Value.(*ticket).class == classUrgent {
				s.WaitingUrgent++
			} else {
				s.WaitingBackground++
			}
		}
	}
	for _, w := range rt.workers {
		s.Workers = append(s.Workers, WorkerStatus{w.cfg.Name, w.healthy, w.cfg.Slots, len(w.active), w.dispatched, w.est[0], w.est[1]})
	}
	return s
}
