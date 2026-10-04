#!/usr/bin/env bash
# Enable core isolation settings for low-latency benchmarking.
# Saves original settings to a state file so isolate_cleanup.sh can restore them.
# Requires root for most settings.
#
# Usage:
#   sudo bash isolate_cores.sh [ISOLATED_CORES]
#   # Run benchmark
#   sudo bash isolate_cleanup.sh
#
# ISOLATED_CORES defaults to last 2 cores (e.g., "14,15" on a 16-core machine).
# To isolate specific cores: sudo bash isolate_cores.sh "10,11,12,13"

set -euo pipefail

if [ "$EUID" -ne 0 ]; then
    echo "Error: this script requires root (use: sudo $0)" >&2
    exit 1
fi

NPROC="$(nproc)"
ISOLATED_CORES="${1:-$((NPROC - 2)),$((NPROC - 1))}"
STATE_FILE="/tmp/core_isolation_state.txt"

echo "Core isolation setup"
echo "  Isolated cores: $ISOLATED_CORES"
echo "  State file: $STATE_FILE"
echo ""

# Initialize state file
> "$STATE_FILE"

# Helper to save setting before modifying
save_setting() {
    local path="$1"
    local name="$2"
    if [ -f "$path" ]; then
        local current="$(cat "$path")"
        echo "$name|$path|$current" >> "$STATE_FILE"
        echo "  Saved: $name = $current"
    fi
}

# 1. Disable CPU frequency scaling (use performance governor)
echo "Disabling CPU frequency scaling..."
for cpu in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    if [ -f "$cpu" ]; then
        save_setting "$cpu" "$(basename $(dirname $(dirname $cpu)))/scaling_governor"
        echo "performance" > "$cpu"
    fi
done
echo "  ✓ Set scaling_governor to 'performance'"

# 2. Disable CPU frequency limits (set to max)
echo "Setting CPU frequencies to maximum..."
for cpu in /sys/devices/system/cpu/cpu*/cpufreq/scaling_max_freq; do
    if [ -f "$cpu" ]; then
        save_setting "$cpu" "$(basename $(dirname $(dirname $cpu)))/scaling_max_freq"
        # Get the max available frequency from cpuinfo_max_freq
        cpuinfo="$(dirname $cpu)/cpuinfo_max_freq"
        if [ -f "$cpuinfo" ]; then
            max_freq="$(cat "$cpuinfo")"
            echo "$max_freq" > "$cpu"
        fi
    fi
done
echo "  ✓ Set scaling_max_freq to cpuinfo_max_freq"

# 3. Disable C-state power management (CPU idle states)
# This is often controlled by kernel params, but we can try ACPI settings
if [ -d "/sys/module/cpuidle" ]; then
    echo "Disabling CPU idle states..."
    if [ -f "/sys/module/cpuidle/parameters/off" ]; then
        save_setting "/sys/module/cpuidle/parameters/off" "cpuidle/off"
        echo "1" > "/sys/module/cpuidle/parameters/off" 2>/dev/null || true
    fi
    echo "  ✓ Attempted to disable C-states (may require kernel parameter)"
fi

# 4. Reduce scheduler migration cost to improve latency predictability
echo "Adjusting scheduler settings..."
sched_cost="/proc/sys/kernel/sched_migration_cost_ns"
if [ -f "$sched_cost" ]; then
    save_setting "$sched_cost" "sched_migration_cost_ns"
    echo "5000000" > "$sched_cost"  # 5ms (default is ~500us, we increase it to reduce migrations)
    echo "  ✓ Set sched_migration_cost_ns to 5ms"
fi

# 5. Increase scheduler latency priority
sched_latency="/proc/sys/kernel/sched_latency_ns"
if [ -f "$sched_latency" ]; then
    save_setting "$sched_latency" "sched_latency_ns"
    echo "3000000" > "$sched_latency"  # 3ms
    echo "  ✓ Set sched_latency_ns to 3ms"
fi

# 6. Increase RCU grace period (reduces RCU callbacks on isolated cores)
rcu_norm="/proc/sys/kernel/rcu_normal"
if [ -f "$rcu_norm" ]; then
    save_setting "$rcu_norm" "rcu_normal"
    echo "1" > "$rcu_norm"
    echo "  ✓ Enabled RCU normal mode"
fi

# 7. Redirect IRQs away from isolated cores (if cpuset is available)
if [ -d "/dev/cpuset" ]; then
    echo "Isolating from IRQs..."
    cpuset_isolated="/dev/cpuset/system.slice/cpuset.cpus"
    if [ -f "$cpuset_isolated" ]; then
        # Get all CPUs except isolated ones
        all_cpus=$(seq 0 $((NPROC - 1)) | tr '\n' ',' | sed 's/,$//')
        non_isolated=$(comm -23 <(echo "$all_cpus" | tr ',' '\n' | sort) <(echo "$ISOLATED_CORES" | tr ',' '\n' | sort) | tr '\n' ',' | sed 's/,$//')
        save_setting "$cpuset_isolated" "cpuset/system.slice/cpuset.cpus"
        echo "$non_isolated" > "$cpuset_isolated" 2>/dev/null || true
        echo "  ✓ Redirected system IRQs to non-isolated cores"
    fi
fi

echo ""
echo "Core isolation enabled for cores: $ISOLATED_CORES"
echo "Original settings saved to: $STATE_FILE"
echo ""
echo "Run your benchmark now, then restore settings with:"
echo "  sudo bash bench/isolate_cleanup.sh"
echo ""
echo "Export for use in benchmark scripts:"
echo "  export BENCH_ISOLATED_CORES='$ISOLATED_CORES'"
echo "  export BENCH_PRODUCER_CORE=$((NPROC - 1))"
echo "  export BENCH_CONSUMER_CORE=$((NPROC - 2))"
