#!/usr/bin/env bash
# Finishes isolating the producer and consumer CPUs for the IPC benchmark.
# The CPUs must already be isolated at boot (see the README's one-time setup):
#
#   isolcpus=domain,managed_irq,<cpus> nohz_full=<cpus> rcu_nocbs=<cpus>
#
# This script checks those took effect, then applies the runtime steps the
# boot parameters don't cover, recording every change in $STATE so
# isolate_cleanup.sh can undo it. Requires root.
#
# Usage: sudo bash bench/isolate_cores.sh [PRODUCER_CPU CONSUMER_CPU]
#
# Without arguments, uses the two boot-isolated CPUs (producer = the higher).
set -euo pipefail

STATE=/run/bench-isolation.state
CPU_SYS=/sys/devices/system/cpu

if [ "$EUID" -ne 0 ]; then
    echo "Error: requires root (use: sudo $0)" >&2
    exit 1
fi
if [ -e "$STATE" ]; then
    echo "Error: $STATE exists — isolation is already applied (or a cleanup failed)." >&2
    echo "Run 'sudo bash bench/isolate_cleanup.sh' first." >&2
    exit 1
fi

# "0-2,5" -> "0 1 2 5"
expand_cpus() {
    local part parts out=()
    IFS=, read -ra parts <<< "$1"
    for part in "${parts[@]}"; do
        if [[ $part == *-* ]]; then
            out+=($(seq "${part%-*}" "${part#*-}"))
        else
            out+=("$part")
        fi
    done
    echo "${out[@]}"
}

# CPU numbers -> kernel hex cpumask, in comma-separated 32-bit groups.
to_mask() {
    local cpu i groups=() parts=()
    local possible
    possible="$(cat "$CPU_SYS/possible")"
    local n=$(( ${possible##*[-,]} / 32 + 1 ))
    for ((i = 0; i < n; i++)); do groups[i]=0; done
    for cpu in "$@"; do groups[cpu / 32]=$(( groups[cpu / 32] | 1 << (cpu % 32) )); done
    for ((i = n - 1; i >= 0; i--)); do parts+=("$(printf '%08x' "${groups[i]}")"); done
    (IFS=,; echo "${parts[*]}")
}

contains() { # contains "list" cpu
    [[ " $1 " == *" $2 "* ]]
}

boot_setup_help() {
    local cpus="$1"
    echo "" >&2
    echo "Add to GRUB_CMDLINE_LINUX_DEFAULT in /etc/default/grub, then run" >&2
    echo "'sudo update-grub' and reboot (see the README's one-time setup):" >&2
    echo "  isolcpus=domain,managed_irq,$cpus nohz_full=$cpus rcu_nocbs=$cpus" >&2
}

# --- Check boot-time isolation -------------------------------------------------

isolated="$(expand_cpus "$(cat "$CPU_SYS/isolated")")"
if [ $# -eq 2 ]; then
    PRODUCER_CPU="$1"
    CONSUMER_CPU="$2"
elif [ $# -eq 0 ]; then
    read -ra isolated_arr <<< "$isolated"
    if [ "${#isolated_arr[@]}" -ne 2 ]; then
        mapfile -t phys_cores < <(sort -un "$CPU_SYS"/cpu[0-9]*/topology/thread_siblings_list)
        suggested="${phys_cores[-2]%%[-,]*},${phys_cores[-1]%%[-,]*}"
        echo "Error: expected exactly 2 boot-isolated CPUs, found: ${isolated:-none}." >&2
        echo "Either pass PRODUCER_CPU CONSUMER_CPU, or set up boot isolation." >&2
        boot_setup_help "$suggested"
        exit 1
    fi
    PRODUCER_CPU="${isolated_arr[1]}"
    CONSUMER_CPU="${isolated_arr[0]}"
else
    echo "Usage: $0 [PRODUCER_CPU CONSUMER_CPU]" >&2
    exit 1
fi
cpus="$CONSUMER_CPU,$PRODUCER_CPU"

for cpu in "$PRODUCER_CPU" "$CONSUMER_CPU"; do
    if [ ! -r "$CPU_SYS/cpu$cpu/topology/thread_siblings_list" ]; then
        echo "Error: CPU $cpu doesn't exist or is offline." >&2
        exit 1
    fi
done
producer_threads="$(expand_cpus "$(cat "$CPU_SYS/cpu$PRODUCER_CPU/topology/thread_siblings_list")")"
if contains "$producer_threads" "$CONSUMER_CPU"; then
    echo "Error: CPUs $PRODUCER_CPU and $CONSUMER_CPU are on the same physical core." >&2
    exit 1
fi

nohz_full="$(expand_cpus "$(cat "$CPU_SYS/nohz_full" 2>/dev/null || true)")"
cmdline="$(cat /proc/cmdline)"
rcu_nocbs=""
if [[ $cmdline =~ (^|[[:space:]])rcu_nocbs=([^[:space:]]+) ]]; then
    rcu_nocbs="$(expand_cpus "${BASH_REMATCH[2]}")"
fi
missing=()
for cpu in "$PRODUCER_CPU" "$CONSUMER_CPU"; do
    contains "$isolated" "$cpu" || missing+=("isolcpus (CPU $cpu)")
    contains "$nohz_full" "$cpu" || missing+=("nohz_full (CPU $cpu)")
    contains "$rcu_nocbs" "$cpu" || missing+=("rcu_nocbs (CPU $cpu)")
done
[[ $cmdline =~ isolcpus=[^[:space:]]*managed_irq ]] || missing+=("isolcpus managed_irq flag")
if [ "${#missing[@]}" -ne 0 ]; then
    echo "Error: boot-time isolation is missing for the benchmark CPUs:" >&2
    printf '  - %s\n' "${missing[@]}" >&2
    boot_setup_help "$cpus"
    exit 1
fi

# --- Runtime steps --------------------------------------------------------------

record() {
    echo "$*" >> "$STATE"
}

# Records the current value, then writes the new one.
set_value() {
    record "write|$1|$(cat "$1")"
    echo "$2" > "$1"
}

siblings=()
for cpu in $producer_threads $(expand_cpus "$(cat "$CPU_SYS/cpu$CONSUMER_CPU/topology/thread_siblings_list")"); do
    if [ "$cpu" != "$PRODUCER_CPU" ] && [ "$cpu" != "$CONSUMER_CPU" ]; then
        siblings+=("$cpu")
    fi
done
housekeeping=()
for cpu in $(expand_cpus "$(cat "$CPU_SYS/online")"); do
    if [ "$cpu" != "$PRODUCER_CPU" ] && [ "$cpu" != "$CONSUMER_CPU" ] && ! contains "${siblings[*]}" "$cpu"; then
        housekeeping+=("$cpu")
    fi
done
housekeeping_list="$(IFS=,; echo "${housekeeping[*]}")"
housekeeping_mask="$(to_mask "${housekeeping[@]}")"

echo "Boot-time isolation OK for CPUs $cpus (isolcpus, nohz_full, rcu_nocbs, managed_irq)"
echo "Isolating producer CPU $PRODUCER_CPU and consumer CPU $CONSUMER_CPU"
echo "  SMT siblings taken offline: ${siblings[*]:-none}"
echo "  Housekeeping CPUs: $housekeeping_list"

trap 'echo "Setup failed; run sudo bash bench/isolate_cleanup.sh to undo partial changes." >&2' ERR

( umask 077; : > "$STATE" )
record "cpus|$PRODUCER_CPU|$CONSUMER_CPU"

for cpu in "${siblings[@]}"; do
    set_value "$CPU_SYS/cpu$cpu/online" 0
done

# irqbalance would undo the IRQ affinities below.
if systemctl is-active --quiet irqbalance 2>/dev/null; then
    systemctl stop irqbalance
    record "start|irqbalance"
fi
set_value /proc/irq/default_smp_affinity "$housekeeping_mask"
moved=0
for irq in /proc/irq/[0-9]*; do
    old="$(cat "$irq/smp_affinity_list")"
    if echo "$housekeeping_list" > "$irq/smp_affinity_list" 2>/dev/null; then
        record "write|$irq/smp_affinity_list|$old"
        moved=$((moved + 1))
    fi
done
echo "  ✓ Moved $moved IRQs to housekeeping CPUs (kernel-managed IRQs are handled by isolcpus=managed_irq)"

set_value /sys/devices/virtual/workqueue/cpumask "$housekeeping_mask"
set_value /proc/sys/kernel/watchdog_cpumask "$housekeeping_list"
echo "  ✓ Unbound workqueues and lockup watchdog moved to housekeeping CPUs"

for cpu in "$PRODUCER_CPU" "$CONSUMER_CPU"; do
    for state in "$CPU_SYS/cpu$cpu"/cpuidle/state*; do
        [ -e "$state" ] || continue
        if [ "$(cat "$state/name")" != POLL ]; then
            set_value "$state/disable" 1
        fi
    done
    freq="$CPU_SYS/cpu$cpu/cpufreq"
    if [ -d "$freq" ]; then
        set_value "$freq/scaling_governor" performance
        set_value "$freq/scaling_max_freq" "$(cat "$freq/cpuinfo_max_freq")"
        set_value "$freq/scaling_min_freq" "$(cat "$freq/cpuinfo_max_freq")"
    fi
done
echo "  ✓ Isolated CPUs: idle states disabled, pinned to max frequency"

trap - ERR
echo ""
echo "Isolation applied; undo with: sudo bash bench/isolate_cleanup.sh"
