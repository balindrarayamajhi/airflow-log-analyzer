#!/usr/bin/env bash
set -euo pipefail

LOG_DIR="${1:-./logs/dag_id=marketvol}"
python3 scripts/log_analyzer.py "$LOG_DIR"
