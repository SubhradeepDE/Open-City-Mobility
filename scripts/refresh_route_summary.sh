#!/bin/bash

set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

cd "$PROJECT_DIR"

source venv/bin/activate

python src/transformation/route_activity_summary.py