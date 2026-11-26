#include "stream.hpp"
#include <cassert>

int main() {
    Stream stream;
    stream.start = Clock::now();
    auto send = [&](std::string data){stream.feed(data.data(), data.size());};
    send("data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\r\n\r\n");
    assert(stream.first < 0);
    send("da");
    send("ta: {\"choices\":[{\"delta\":{\"content\":\"A\"}}]}\r\n");
    assert(stream.first < 0);
    send("\r\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DO");
    assert(stream.first >= 0 && !stream.done);
    send("NE]\n\n");
    assert(stream.done);

    Stream broken;
    broken.start = Clock::now();
    std::string partial = "data: {\"choices\":[{\"delta\":{\"content\":\"A\"}}]}\n\ndata: [DO";
    broken.feed(partial.data(), partial.size());
    assert(broken.first >= 0 && !broken.done); // Truncated before [DONE] must not count as done.

    std::string worker;
    assert(!header_value("Content-Type: text/event-stream\r\n", "X-Inference-Worker", worker));
    assert(!header_value("X-Inference-Worker-Id: 7\r\n", "X-Inference-Worker", worker));
    assert(!header_value("HTTP/1.1 200 OK\r\n", "X-Inference-Worker", worker));
    assert(worker.empty() && worker_label(worker) == "unknown");
    assert(header_value("x-inference-worker:  backup \r\n", "X-Inference-Worker", worker) && worker == "backup");
    assert(header_value("X-INFERENCE-WORKER:primary\n", "X-Inference-Worker", worker) && worker == "primary");
    assert(header_value("X-Inference-Worker: \r\n", "X-Inference-Worker", worker) && worker_label(worker) == "unknown");
    assert(worker_label("rog,backup\n") == "rog_backup_");

    // Role-only first event must not set TTFT, even when it is the only event before a break.
    Stream role_only;
    role_only.start = Clock::now();
    std::string role = "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n";
    role_only.feed(role.data(), role.size());
    assert(role_only.first < 0);

    // Failover measurement on one origin-relative clock.
    auto none = first_backup_after({{0, "primary", 100, 40}, {1, "unknown", 12000, 50}, {2, "backup", -1, -1}}, 10000);
    assert(none.id < 0 && none.latency_ms < 0 && none.backup_before_signal == 0);
    auto m = first_backup_after({{0, "backup", 9000, 30},       // before the signal: warned, not used
                                 {11, "backup", 12500, 1500},
                                 {10, "backup", 11800, 1800},   // earliest after the signal
                                 {12, "primary", 10100, 20}}, 10000);
    assert(m.id == 10 && m.first_content_ms == 11800 && m.ttft_ms == 1800 && m.latency_ms == 1800);
    assert(m.backup_before_signal == 1);
    auto broken_backup = first_backup_after({{5, "backup", 10400, 400}}, 10000); // Content counts even if stream later broke.
    assert(broken_backup.id == 5 && broken_backup.latency_ms == 400);

    // vLLM /metrics parsing: exact family names, sum counts across engines, max KV usage.
    auto g = parse_vllm_metrics(
        "# HELP vllm:num_requests_running Number of requests in model execution batches.\n"
        "# TYPE vllm:num_requests_running gauge\n"
        "vllm:num_requests_running{engine=\"0\",model_name=\"Qwen/Qwen2.5-0.5B-Instruct\"} 3.0\n"
        "vllm:num_requests_running{engine=\"1\",model_name=\"a b}c\"} 2\n"
        "vllm:num_requests_waiting{engine=\"0\"} 7.0\n"
        "vllm:num_requests_waiting_by_reason{engine=\"0\",reason=\"capacity\"} 99\n"
        "vllm:kv_cache_usage_perc{engine=\"0\"} 0.25\n"
        "vllm:kv_cache_usage_perc{engine=\"1\"} 0.5\r\n"
        "vllm:num_preemptions_total{engine=\"0\"} 4 1700000000000\n"
        "vllm:num_requests_running_garbage 1e309\n"
        "vllm:num_requests_running{engine=\"2\"} NaN\n");
    assert(g.running && *g.running == 5);
    assert(g.waiting && *g.waiting == 7);
    assert(g.kv_cache_usage && *g.kv_cache_usage == 0.5);
    assert(g.preemptions_total && *g.preemptions_total == 4);
    auto sim_page = parse_vllm_metrics("vllm:num_requests_running 1\n");  // Simulator-style page.
    assert(sim_page.running && !sim_page.waiting && !sim_page.kv_cache_usage && !sim_page.preemptions_total);
    auto legacy = parse_vllm_metrics("vllm:gpu_cache_usage_perc{engine=\"0\"} 0.75\n");
    assert(legacy.kv_cache_usage && *legacy.kv_cache_usage == 0.75);
    auto empty = parse_vllm_metrics("404 page not found\n");
    assert(!empty.running && !empty.waiting && !empty.kv_cache_usage);

    assert(percentile({}, .95) == -1);
    assert(percentile({5, 1, 3, 2, 4}, .5) == 3 && percentile({5, 1, 3, 2, 4}, 1.0) == 5);
    assert(percentile({5, 1, 3, 2, 4}, 0) == 1);
}
