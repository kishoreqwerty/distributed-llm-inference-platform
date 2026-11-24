#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <signal.h>
#include <unistd.h>
#ifdef __APPLE__
#include <sys/sysctl.h>
#endif
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <chrono>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "stream.hpp"

struct Job {
    int offset, tokens;
    std::string priority, prompt;
};

struct Result {
    double dispatch = 0, ttft = -1, elapsed = 0;
    long http = 0;
    std::string outcome = "failed", detail, worker = "unknown";
    // Status 200 arrived (stream began) but it ended before [DONE].
    bool interrupted() const { return http == 200 && outcome == "failed"; }
    // First nonempty content token, ms since origin (same clock as dispatch_ms and the signal).
    double first_content() const { return ttft >= 0 ? dispatch + ttft : -1; }
};

// Optional shutdown signal, sent by this process so it shares the requests' monotonic clock.
struct Shutdown {
    pid_t pid = 0;
    int at_ms = -1;
    double sent_ms = -1;   // Timestamp taken immediately before kill(), ms since origin.
    double exit_ms = -1;   // First poll (20 ms period) that found the process gone or a zombie.
    std::string error;
    bool enabled() const { return pid > 0; }
};

enum class Proc { gone, zombie, running };

// Process state plus whether it ignores SIGINT (in which case the shutdown would silently do nothing).
Proc inspect(pid_t pid, bool* sigint_ignored = nullptr) {
    if (sigint_ignored) *sigint_ignored = false;
#ifdef __APPLE__
    int mib[4] = {CTL_KERN, KERN_PROC, KERN_PROC_PID, pid};
    kinfo_proc info{};
    size_t size = sizeof info;
    if (sysctl(mib, 4, &info, &size, nullptr, 0) != 0 || size == 0) return Proc::gone;
    if (sigint_ignored) *sigint_ignored = info.kp_proc.p_sigignore & sigmask(SIGINT);
    return info.kp_proc.p_stat == SZOMB ? Proc::zombie : Proc::running;
#else
    return kill(pid, 0) == 0 || errno == EPERM ? Proc::running : Proc::gone;
#endif
}

size_t receive(char* bytes, size_t size, size_t count, void* user) {
    auto& stream = *static_cast<Stream*>(user);
    try { stream.feed(bytes, size * count); }
    catch (const std::exception& e) { stream.error = e.what(); return 0; }
    return size * count;
}

// Records X-Inference-Worker from the final response's headers. libcurl delivers headers before
// any body bytes, so the value survives a stream that breaks later.
size_t receive_header(char* bytes, size_t size, size_t count, void* user) {
    auto& worker = *static_cast<std::string*>(user);
    std::string_view line(bytes, size * count);
    if (line.starts_with("HTTP/")) worker.clear(); // New response (e.g. 100 Continue): keep only the last.
    else header_value(line, "X-Inference-Worker", worker);
    return size * count;
}

void run_job(const Job& job, Result& result, Clock::time_point origin,
             const std::string& url, const std::string& model) {
    using Handle = std::unique_ptr<CURL, decltype(&curl_easy_cleanup)>;
    Handle curl(curl_easy_init(), curl_easy_cleanup);
    if (!curl) throw std::runtime_error("curl init failed");
    Json payload = {{"model", model}, {"stream", true}, {"temperature", 0},
                    {"max_tokens", job.tokens}, {"messages", Json::array({
                        {{"role","user"}, {"content",job.prompt}}})}};
    auto body = payload.dump();
    curl_slist* raw = nullptr;
    for (auto header : {std::string("Content-Type: application/json"), "X-Request-Priority: " + job.priority}) {
        auto next = curl_slist_append(raw, header.c_str());
        if (!next) { curl_slist_free_all(raw); throw std::bad_alloc(); }
        raw = next;
    }
    std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> headers(raw, curl_slist_free_all);
    Stream stream;
    std::string worker;
    curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, raw);
    curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl.get(), CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl.get(), CURLOPT_TIMEOUT, 135L);
    curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, receive);
    curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &stream);
    curl_easy_setopt(curl.get(), CURLOPT_HEADERFUNCTION, receive_header);
    curl_easy_setopt(curl.get(), CURLOPT_HEADERDATA, &worker);
    std::this_thread::sleep_until(origin + Millis(job.offset));
    stream.start = Clock::now();
    result.dispatch = milliseconds(stream.start - origin);
    auto rc = curl_easy_perform(curl.get());
    result.elapsed = milliseconds(Clock::now() - stream.start);
    curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &result.http);
    result.ttft = stream.first;
    result.worker = worker_label(worker);
    if (result.http == 429) result.outcome = "rejected";
    else if (rc == CURLE_OK && result.http == 200 && stream.done && stream.first >= 0)
        result.outcome = "ok";
    else if (!stream.error.empty()) result.detail = stream.error;
    else if (rc != CURLE_OK) result.detail = curl_easy_strerror(rc);
    else result.detail = "missing successful status, content or DONE";
}

// rate > 0: fixed open-loop arrivals at i * 1000 / rate ms. Otherwise the original 20-30 ms jitter.
std::vector<Job> workload(int count, unsigned seed, double rate) {
    std::mt19937 rng(seed);
    const std::vector<std::string> prompts = {"Explain a GPU in one sentence.",
        "Explain what a queue does.", "Give an example of a background job."};
    std::vector<Job> jobs;
    int offset = 0;
    for (int i = 0; i < count; ++i) {
        bool urgent = i % 4 == 2;
        jobs.push_back({offset, urgent ? 4 : 10, urgent ? "urgent" : "background",
                        prompts[rng() % prompts.size()]});
        offset += 20 + rng() % 11;
        if (rate > 0) offset = static_cast<int>(std::llround((i + 1) * 1000.0 / rate));
    }
    return jobs;
}

void report(const std::vector<Job>& jobs, const std::vector<Result>& results,
            double seconds, int target, const Shutdown& shutdown) {
    std::cout << "measurement_window_s=" << seconds << "\n";
    for (std::string cls : {"urgent", "background"}) {
        int offered = 0, rejected = 0, hits = 0;
        std::vector<double> times;
        for (size_t i = 0; i < jobs.size(); ++i) {
            if (jobs[i].priority != cls) continue;
            const auto& r = results[i];
            ++offered;
            if (r.outcome == "rejected") ++rejected;
            if (r.outcome == "ok") {
                times.push_back(r.ttft);
                if (r.ttft <= target) ++hits;
            }
        }
        std::sort(times.begin(), times.end());
        std::cout << cls << " offered=" << offered << " ok=" << times.size()
                  << " rejected=" << rejected << " failed=" << offered-times.size()-rejected;
        if (!times.empty()) {
            auto percentile = [&](double q){return times.at(std::ceil(q * times.size()) - 1);};
            std::cout << " p50_ttft_ms=" << percentile(.50) << " p95_ttft_ms=" << percentile(.95);
        }
        if (cls == "urgent" && offered)
            std::cout << " target_ms=" << target << " within_target_pct=" << 100.0*hits/offered;
        if (cls == "background") std::cout << " completions_per_sec=" << times.size()/seconds;
        std::cout << "\n";
    }
    int ok = 0, rejected = 0, failed = 0, interrupted = 0;
    std::map<std::string, int> ok_by_worker = {{"primary", 0}, {"backup", 0}}, failed_by_worker, interrupted_by_worker;
    for (const auto& r : results) {
        if (r.outcome == "ok") { ++ok; ++ok_by_worker[r.worker]; }
        else if (r.outcome == "rejected") ++rejected;
        else { ++failed; ++failed_by_worker[r.worker]; }
        if (r.interrupted()) { ++interrupted; ++interrupted_by_worker[r.worker]; }
    }
    std::cout << "total offered=" << results.size() << " ok=" << ok << " rejected=" << rejected
              << " failed=" << failed << " interrupted_streams=" << interrupted << "\nok_by_worker";
    for (const auto& [w, n] : ok_by_worker) std::cout << ' ' << w << '=' << n;
    std::cout << "\nfailed_by_worker";
    for (const auto& [w, n] : failed_by_worker) std::cout << ' ' << w << '=' << n;
    std::cout << "\ninterrupted_by_worker";
    for (const auto& [w, n] : interrupted_by_worker) std::cout << ' ' << w << '=' << n;
    std::cout << "\n";
    if (!shutdown.enabled()) return;
    std::cout << "shutdown pid=" << shutdown.pid << " signal=SIGINT scheduled_ms=" << shutdown.at_ms;
    if (!shutdown.error.empty()) {
        std::cout << " sent=no error=\"" << shutdown.error << "\"\nfailover not_measured (signal was not sent)\n";
        return;
    }
    std::cout << " sent=yes signal_ms=" << shutdown.sent_ms;
    if (shutdown.exit_ms >= 0) std::cout << " primary_exited=yes exit_observed_ms=" << shutdown.exit_ms
                                         << " signal_to_exit_ms=" << shutdown.exit_ms - shutdown.sent_ms << "\n";
    else std::cout << " primary_exited=NO\nwarning PRIMARY_STILL_RUNNING: PID " << shutdown.pid
                   << " did not exit after SIGINT; failover was not exercised by the shutdown\n";
    std::vector<ContentEvent> events;
    for (size_t i = 0; i < results.size(); ++i)
        events.push_back({static_cast<int>(i), results[i].worker, results[i].first_content(), results[i].ttft});
    auto m = first_backup_after(events, shutdown.sent_ms);
    if (m.id < 0) std::cout << "failover NO_BACKUP_CONTENT: no backup content token arrived after the shutdown signal\n";
    else std::cout << "failover shutdown_to_first_backup_content_ms=" << m.latency_ms
                   << " first_backup_request=" << m.id << " first_backup_content_ms=" << m.first_content_ms
                   << " first_backup_request_ttft_ms=" << m.ttft_ms << "\n";
    if (m.backup_before_signal)
        std::cout << "warning backup_content_before_signal=" << m.backup_before_signal
                  << " (primary was already failing over before the signal)\n";
}

int main(int argc, char** argv) try {
    if (argc < 2 || argc > 10 || argc == 9) {
        std::cerr << "usage: loadgen CSV [URL [MODEL [REQUESTS [SEED [TARGET_MS [RATE_RPS [SIGINT_PID SIGINT_AT_MS]]]]]]]\n";
        return 2;
    }
    std::string url = argc > 2 ? argv[2] : "http://127.0.0.1:8080/v1/chat/completions";
    std::string model = argc > 3 ? argv[3] : "Qwen/Qwen2.5-0.5B-Instruct";
    int count = argc > 4 ? std::stoi(argv[4]) : 48;
    unsigned seed = argc > 5 ? std::stoul(argv[5]) : 42;
    int target = argc > 6 ? std::stoi(argv[6]) : 500;
    double rate = argc > 7 ? std::stod(argv[7]) : 0;
    if (count < 4 || count > 256 || target < 1) throw std::runtime_error("use 4..256 requests and positive target");
    if (argc > 7 && !(rate > 0 && rate <= 1000)) throw std::runtime_error("RATE_RPS must be in (0, 1000]");
    Shutdown shutdown;
    if (argc > 9) {
        long pid = std::stol(argv[8]);
        shutdown.at_ms = std::stoi(argv[9]);
        if (pid <= 1 || pid == getpid() || pid == getppid() || shutdown.at_ms < 0)
            throw std::runtime_error("SIGINT_PID must be another process (> 1) and SIGINT_AT_MS >= 0");
        shutdown.pid = static_cast<pid_t>(pid);
        if (kill(shutdown.pid, 0) != 0)
            throw std::runtime_error("SIGINT_PID " + std::to_string(pid) + " not signalable: " + std::strerror(errno));
        bool ignored = false;
        if (inspect(shutdown.pid, &ignored) != Proc::running) throw std::runtime_error("SIGINT_PID is not running");
        if (ignored) throw std::runtime_error("SIGINT_PID " + std::to_string(pid) +
            " ignores SIGINT (e.g. started with nohup or & from a script); it would not shut down");
    }
    std::ofstream csv(argv[1]);
    csv.exceptions(std::ios::badbit | std::ios::failbit);
    auto jobs = workload(count, seed, rate);
    std::vector<Result> results(count);
    if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) throw std::runtime_error("curl initialization failed");
    auto origin = Clock::now() + Millis(250);
    {
        std::mutex lock;
        std::condition_variable wake;
        bool cancelled = false;                 // Guarded by lock: never signal if the run failed to start.
        std::atomic<bool> finished = false;     // All request threads joined.
        std::thread signaller;
        if (shutdown.enabled()) signaller = std::thread([&]{
            {
                std::unique_lock held(lock);
                if (wake.wait_until(held, origin + Millis(shutdown.at_ms), [&]{ return cancelled; })) return;
            }
            auto now = Clock::now();
            if (kill(shutdown.pid, SIGINT) != 0) { shutdown.error = std::strerror(errno); return; }
            shutdown.sent_ms = milliseconds(now - origin);
            auto give_up = Clock::now() + std::chrono::minutes(2);
            std::optional<Clock::time_point> grace;  // Keep watching ~1 s after the requests finish.
            while (Clock::now() < give_up && (!grace || Clock::now() < *grace)) {
                if (inspect(shutdown.pid) != Proc::running) { shutdown.exit_ms = milliseconds(Clock::now() - origin); return; }
                std::this_thread::sleep_for(Millis(20));
                if (finished && !grace) grace = Clock::now() + Millis(1000);
            }
        });
        std::vector<std::thread> clients;
        auto join = [&]{ for (auto& t : clients) t.join(); finished = true; if (signaller.joinable()) signaller.join(); };
        try {
            for (int i=0; i<count; ++i) clients.emplace_back([&,i]{
                try { run_job(jobs[i], results[i], origin, url, model); }
                catch (const std::exception& e) { results[i].detail = e.what(); }
            });
        } catch (...) {
            { std::lock_guard held(lock); cancelled = true; }
            wake.notify_all();
            join();
            throw;
        }
        join();
    }
    double window_ms = 0;
    csv << "id,priority,scheduled_ms,dispatch_ms,max_tokens,ttft_ms,elapsed_ms,http,outcome,worker,first_content_ms,interrupted\n";
    bool failed = false;
    for (int i=0; i<count; ++i) {
        auto& j = jobs[i]; auto& r = results[i];
        window_ms = std::max(window_ms, r.dispatch + r.elapsed);
        csv << i << ',' << j.priority << ',' << j.offset << ',' << r.dispatch << ',' << j.tokens << ',';
        if (r.ttft >= 0) csv << r.ttft;
        csv << ',' << r.elapsed << ',' << r.http << ',' << r.outcome << ',' << r.worker << ',';
        if (r.ttft >= 0) csv << r.first_content();
        csv << ',' << (r.interrupted() ? 1 : 0) << '\n';
        if (r.outcome == "failed") { failed = true; std::cerr << "request " << i << ": " << r.detail << '\n'; }
    }
    csv.close();
    std::cout << "model=" << model << " seed=" << seed << " csv=" << argv[1] << "\n";
    if (rate > 0) std::cout << "rate_rps=" << rate << " arrivals=open-loop\n";
    report(jobs, results, std::max(window_ms, .001)/1000, target, shutdown);
    curl_global_cleanup();
    return failed ? 1 : 0;
} catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
