package main

import (
	"flag"
	"inferenceplatform/internal/gateway"
	"log"
	"net/http"
	"net/url"
	"time"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:8080", "gateway address")
	worker := flag.String("worker", "http://127.0.0.1:8000", "worker base URL")
	backup := flag.String("backup-worker", "", "optional failover worker base URL")
	policy := flag.String("policy", "fifo", "fifo or priority")
	slots := flag.Int("slots", 1, "maximum active requests")
	limit := flag.Int("queue", 128, "maximum waiting requests")
	timeout := flag.Duration("timeout", 2*time.Minute, "queue plus inference deadline")
	route := flag.String("route", "", "opt-in multi-worker routing: primary-only, round-robin or load-aware (needs -workers; replaces -worker, -backup-worker and -slots)")
	workers := flag.String("workers", "", "JSON routing config: workers with URLs, slots and time estimates (used with -route)")
	flag.Parse()
	if (*policy != "fifo" && *policy != "priority") || *limit < 0 || *timeout <= 0 {
		log.Fatal("invalid policy, slots, queue or timeout")
	}
	srv := http.Server{Addr: *addr, ReadTimeout: 10 * time.Second, IdleTimeout: time.Minute}
	if *route != "" || *workers != "" {
		mode, err := gateway.ParseMode(*route)
		if err != nil || *workers == "" {
			log.Fatal("-route and -workers must be used together: ", err)
		}
		flag.Visit(func(f *flag.Flag) {
			if f.Name == "worker" || f.Name == "backup-worker" || f.Name == "slots" {
				log.Fatalf("-%s cannot be combined with -route; set workers and slots in %s", f.Name, *workers)
			}
		})
		cfg, err := gateway.LoadRoutingConfig(*workers)
		if err != nil {
			log.Fatal(err)
		}
		rt, err := gateway.NewRouter(mode, cfg, *limit, *policy == "priority")
		if err != nil {
			log.Fatal(err)
		}
		defer rt.Start()()
		for _, w := range cfg.Workers {
			log.Printf("worker %s url=%s slots=%d urgent=%+v background=%+v", w.Name, w.URL, w.Slots, w.Urgent, w.Background)
		}
		log.Printf("gateway %s policy=%s route=%s workers=%d queue=%d ewma_alpha=%g", *addr, *policy, mode, len(cfg.Workers), *limit, cfg.EWMAAlpha)
		srv.Handler = gateway.RoutedHandler(rt, *timeout)
		log.Fatal(srv.ListenAndServe())
	}
	for _, address := range []string{*worker, *backup} {
		if address == "" && *worker != "" {
			continue
		}
		u, err := url.Parse(address)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" {
			log.Fatal("invalid worker base URL")
		}
	}
	if *slots < 1 {
		log.Fatal("invalid policy, slots, queue or timeout")
	}
	q := gateway.NewQueue(*slots, *limit, *policy == "priority")
	log.Printf("gateway %s policy=%s slots=%d worker=%s", *addr, *policy, *slots, *worker)
	srv.Handler = gateway.HandlerWithBackup(*worker, *backup, q, *timeout)
	log.Fatal(srv.ListenAndServe())
}
