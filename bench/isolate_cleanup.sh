#!/usr/bin/env bash
# Restore core isolation settings saved by isolate_cores.sh.
# Requires root.
#
# Usage:
#   sudo bash isolate_cleanup.sh

set -euo pipefail

if [ "$EUID" -ne 0 ]; then
    echo "Error: this script requires root (use: sudo $0)" >&2
    exit 1
fi

STATE_FILE="/tmp/core_isolation_state.txt"

if [ ! -f "$STATE_FILE" ]; then
    echo "Error: state file not found at $STATE_FILE" >&2
    echo "Did you run 'sudo bash bench/isolate_cores.sh' first?" >&2
    exit 1
fi

echo "Restoring core isolation settings from: $STATE_FILE"
echo ""

restored=0
errors=0

while IFS='|' read -r name path value; do
    if [ -z "$name" ] || [ -z "$path" ]; then
        continue
    fi

    if [ ! -e "$path" ]; then
        echo "  ⚠ Skipped (path missing): $name"
        ((errors++))
        continue
    fi

    if echo "$value" > "$path" 2>/dev/null; then
        echo "  ✓ Restored: $name = $value"
        ((restored++))
    else
        echo "  ✗ Failed to restore: $name (permission denied or invalid value)"
        ((errors++))
    fi
done < "$STATE_FILE"

echo ""
if [ $restored -gt 0 ]; then
    echo "Successfully restored $restored settings"
fi
if [ $errors -gt 0 ]; then
    echo "Failed to restore $errors settings (may require elevated privileges)"
fi

rm -f "$STATE_FILE"
echo ""
echo "Core isolation disabled. Settings restored to baseline."
