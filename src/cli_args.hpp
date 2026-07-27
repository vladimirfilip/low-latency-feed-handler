#pragma once

#include <map>
#include <string>
#include <vector>

// Minimal `--key=value` / `--flag` / positional-arg parser shared by
// feed_handler, the IPC producers, and the replay tool — none of these
// binaries need more than this to choose an ingestion mode and pass its
// runtime parameters (host, port, pacing).
struct ParsedArgs {
    std::map<std::string, std::string> options;
    std::vector<std::string> flags;
    std::vector<std::string> positional;

    bool has_flag(const std::string& name) const {
        for (const auto& f : flags) {
            if (f == name) return true;
        }
        return false;
    }

    std::string get(const std::string& key, const std::string& fallback) const {
        auto it = options.find(key);
        return it == options.end() ? fallback : it->second;
    }
};

inline ParsedArgs parse_args(int argc, char** argv) {
    ParsedArgs result;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.rfind("--", 0) == 0) {
            auto eq = arg.find('=');
            if (eq != std::string::npos) {
                result.options[arg.substr(2, eq - 2)] = arg.substr(eq + 1);
            } else {
                result.flags.push_back(arg.substr(2));
            }
        } else {
            result.positional.push_back(arg);
        }
    }
    return result;
}
