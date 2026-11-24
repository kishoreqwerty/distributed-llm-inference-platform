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
}
