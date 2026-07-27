#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${ROOT_DIR}/data"
URL="https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/03272019.NASDAQ_ITCH50.gz"
GZ_PATH="${DATA_DIR}/03272019.NASDAQ_ITCH50.gz"
ITCH_PATH="${DATA_DIR}/03272019.NASDAQ_ITCH50"
SLICE_PATH="${ITCH_PATH}.200MB"

mkdir -p "${DATA_DIR}"

echo "Downloading NASDAQ sample ITCH 5.0 feed (~5.2GB)..."
curl -fSL --progress-bar -o "${GZ_PATH}" "${URL}"

echo "Decompressing to ${ITCH_PATH} (~12GB)..."
gunzip -k -f "${GZ_PATH}"

echo "Slicing first 200MB to ${SLICE_PATH} (the default for feed_handler/ipc_producer_*)..."
head -c 209715200 "${ITCH_PATH}" > "${SLICE_PATH}"

echo "Done."
