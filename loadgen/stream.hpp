#pragma once
#include <cctype>
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
