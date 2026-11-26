#pragma once
#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <optional>
#include <chrono>
#include <string>
#include <string_view>
#include <vector>
#include <stdexcept>
#include <nlohmann/json.hpp>

using Json = nlohmann::json;
using Clock = std::chrono::steady_clock;
using Millis = std::chrono::milliseconds;

inline double milliseconds(Clock::duration duration) {
    return std::chrono::duration<double, std::milli>(duration).count();
}

struct Stream {
    Clock::time_point start;
    double first = -1;
    bool done = false;
    std::string pending, data, error;

    void finish_event() {
        if (data.empty()) return;
        data.pop_back(); // Final newline joining SSE data fields.
        if (data == "[DONE]") {
            done = true;
        } else {
            auto message = Json::parse(data);
            if (message.contains("error")) throw std::runtime_error("worker error event");
            const auto& choices = message.at("choices");
            if (!choices.empty()) {
                const auto& delta = choices.at(0).at("delta");
                if (first < 0 && delta.contains("content") && delta["content"].is_string()
                    && !delta["content"].get_ref<const std::string&>().empty()) {
                    first = milliseconds(Clock::now() - start);
                }
            }
        }
        data.clear();
    }

    void feed(const char* bytes, size_t count) {
        pending.append(bytes, count);
        size_t pos = 0, end;
        while ((end = pending.find('\n', pos)) != std::string::npos) {
            auto line = pending.substr(pos, end - pos);
            pos = end + 1;
            if (!line.empty() && line.back() == '\r') line.pop_back();
            if (line.empty()) finish_event();
            else if (line.starts_with("data:")) {
                auto value = line.substr(5);
                if (!value.empty() && value.front() == ' ') value.erase(0, 1);
                data += value + "\n";
            }
        }
        pending.erase(0, pos);
        if (pending.size() + data.size() > (1u << 20)) throw std::runtime_error("overlarge SSE event");
    }
};

// If `line` is the header `name` (matched case-insensitively), stores its trimmed value.
inline bool header_value(std::string_view line, std::string_view name, std::string& value) {
    auto colon = line.find(':');
    if (colon != name.size()) return false;
    for (size_t i = 0; i < colon; ++i)
        if (std::tolower(static_cast<unsigned char>(line[i])) != std::tolower(static_cast<unsigned char>(name[i])))
            return false;
    auto rest = line.substr(colon + 1);
    auto space = [](char c){ return c == ' ' || c == '\t' || c == '\r' || c == '\n'; };
    while (!rest.empty() && space(rest.front())) rest.remove_prefix(1);
    while (!rest.empty() && space(rest.back())) rest.remove_suffix(1);
    value.assign(rest);
    return true;
}

// CSV-safe worker label: "unknown" when absent or empty, unexpected characters replaced.
inline std::string worker_label(std::string value) {
    if (value.empty()) return "unknown";
    for (auto& c : value)
        if (!std::isalnum(static_cast<unsigned char>(c)) && c != '-' && c != '_' && c != '.') c = '_';
    return value;
}


// One request's first nonempty content token, on the run's monotonic origin-relative clock.
struct ContentEvent {
    int id;
    std::string worker;
    double first_content_ms, ttft_ms;
};

struct FailoverMeasure {
    int id = -1;               // Request that delivered the first backup content after the signal; -1 if none.
    double first_content_ms = -1, ttft_ms = -1, latency_ms = -1;
    int backup_before_signal = 0;
};

// First backup content at or after the shutdown signal. All times share the same origin.
inline FailoverMeasure first_backup_after(const std::vector<ContentEvent>& events, double signal_ms) {
    FailoverMeasure m;
    for (const auto& e : events) {
        if (e.worker != "backup" || e.first_content_ms < 0) continue;
        if (e.first_content_ms < signal_ms) { ++m.backup_before_signal; continue; }
        if (m.id < 0 || e.first_content_ms < m.first_content_ms) {
            m.id = e.id; m.first_content_ms = e.first_content_ms; m.ttft_ms = e.ttft_ms;
        }
    }
    if (m.id >= 0) m.latency_ms = m.first_content_ms - signal_ms;
    return m;
}

// Worker gauges parsed from a vLLM Prometheus /metrics page. Absent families stay empty
// ("unavailable"). Counts are summed across label sets (e.g. engines); KV usage takes the max.
struct WorkerGauges {
    std::optional<double> running, waiting, kv_cache_usage, preemptions_total;
};

inline WorkerGauges parse_vllm_metrics(std::string_view text) {
    WorkerGauges g;
    auto add = [](std::optional<double>& slot, double v, bool use_max) {
        slot = !slot ? v : use_max ? std::max(*slot, v) : *slot + v;
    };
    while (!text.empty()) {
        auto eol = text.find('\n');
        auto line = text.substr(0, eol);
        text = eol == std::string_view::npos ? std::string_view{} : text.substr(eol + 1);
        if (line.empty() || line.front() == '#') continue;
        auto name_end = line.find_first_of("{ \t");
        if (name_end == std::string_view::npos) continue;
        auto name = line.substr(0, name_end);
        auto rest = line.substr(name_end);
        if (!rest.empty() && rest.front() == '{') {
            auto close = rest.rfind('}');
            if (close == std::string_view::npos) continue;
            rest = rest.substr(close + 1);
        }
        auto start = rest.find_first_not_of(" \t");
        if (start == std::string_view::npos) continue;
        rest = rest.substr(start);
        std::string token(rest.substr(0, rest.find_first_of(" \t\r")));
        char* end = nullptr;
        double v = std::strtod(token.c_str(), &end);
        if (token.empty() || end != token.c_str() + token.size() || !std::isfinite(v)) continue;
        if (name == "vllm:num_requests_running") add(g.running, v, false);
        else if (name == "vllm:num_requests_waiting") add(g.waiting, v, false);
        else if (name == "vllm:kv_cache_usage_perc" || name == "vllm:gpu_cache_usage_perc") add(g.kv_cache_usage, v, true);
        else if (name == "vllm:num_preemptions_total") add(g.preemptions_total, v, false);
    }
    return g;
}

// Nearest-rank percentile of an unsorted sample; -1 when empty.
inline double percentile(std::vector<double> values, double q) {
    if (values.empty()) return -1;
    std::sort(values.begin(), values.end());
    auto rank = static_cast<size_t>(std::ceil(q * values.size()));
    return values.at(rank == 0 ? 0 : rank - 1);
}
