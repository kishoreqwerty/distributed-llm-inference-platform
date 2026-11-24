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
	flag.Parse()
	for _, address := range []string{*worker, *backup} {
		if address == "" && *worker != "" {
			continue
		}
		u, err := url.Parse(address)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" {
			log.Fatal("invalid worker base URL")
		}
	}
	if (*policy != "fifo" && *policy != "priority") || *slots < 1 || *limit < 0 || *timeout <= 0 {
		log.Fatal("invalid policy, slots, queue or timeout")
	}
	q := gateway.NewQueue(*slots, *limit, *policy == "priority")
	log.Printf("gateway %s policy=%s slots=%d worker=%s", *addr, *policy, *slots, *worker)
	srv := http.Server{Addr: *addr, Handler: gateway.HandlerWithBackup(*worker, *backup, q, *timeout), ReadTimeout: 10 * time.Second, IdleTimeout: time.Minute}
	log.Fatal(srv.ListenAndServe())
}
