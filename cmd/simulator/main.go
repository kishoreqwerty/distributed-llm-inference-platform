package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"inferenceplatform/internal/gateway"
	"log"
	"net/http"
	"time"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:8000", "worker address")
	first := flag.Duration("first", 40*time.Millisecond, "delay to first content")
	step := flag.Duration("step", 10*time.Millisecond, "delay between content events")
	flag.Parse()
	if *first < 0 || *step < 0 {
		log.Fatal("negative delay")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+gateway.Path, func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Model     string
			Stream    bool
			MaxTokens int `json:"max_tokens"`
		}
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in) != nil || !in.Stream || in.MaxTokens < 1 || in.MaxTokens > 1024 {
			http.Error(w, "expected stream=true and max_tokens from 1 to 1024", 400)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flush := http.NewResponseController(w).Flush
		event := func(delta map[string]string, reason any) bool {
			chunk := map[string]any{"model": in.Model, "object": "chat.completion.chunk", "choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": reason}}}
			data, _ := json.Marshal(chunk)
			_, err := fmt.Fprintf(w, "data: %s\n\n", data)
			return err == nil && flush() == nil
		}
		if !event(map[string]string{"role": "assistant", "content": ""}, nil) {
			return
		}
		for i := 0; i < in.MaxTokens; i++ {
			delay := *step
			if i == 0 {
				delay = *first
			}
			select {
			case <-r.Context().Done():
				return
			case <-time.After(delay):
			}
			if !event(map[string]string{"content": " token"}, nil) {
				return
			}
		}
		if event(map[string]string{}, "length") {
			fmt.Fprint(w, "data: [DONE]\n\n")
			flush()
		}
	})
	log.Printf("simulator %s first=%s step=%s", *addr, *first, *step)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
