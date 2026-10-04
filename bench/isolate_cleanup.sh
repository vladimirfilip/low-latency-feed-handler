#!/usr/bin/env bash
# Undoes isolate_cores.sh, replaying its recorded changes in reverse order.
# Safe to rerun: if anything fails, the state file is kept so a second run
# can retry. Requires root.
#
# Usage: sudo bash bench/isolate_cleanup.sh
set -uo pipefail

STATE=/run/bench-isolation.state

if [ "$EUID" -ne 0 ]; then
    echo "Error: requires root (use: sudo $0)" >&2
    exit 1
fi
if [ ! -f "$STATE" ]; then
    echo "No isolation state at $STATE; nothing to restore."
    exit 0
fi

failed=0
while IFS='|' read -r kind target value; do
    case "$kind" in
        write)
            echo "$value" > "$target" 2>/dev/null || { echo "  ✗ $target -> $value" >&2; failed=$((failed + 1)); } ;;
        start)
            systemctl start "$target" || { echo "  ✗ couldn't start $target" >&2; failed=$((failed + 1)); } ;;
    esac
done < <(tac "$STATE")

if [ "$failed" -ne 0 ]; then
    echo "$failed setting(s) failed to restore; state kept at $STATE — rerun to retry." >&2
    exit 1
fi
rm -f "$STATE"
echo "Isolation removed; all settings restored."
