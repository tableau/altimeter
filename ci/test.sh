#!/usr/bin/env bash

set -ef -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ALTI_DIR=$(dirname "$SCRIPT_DIR")
export PYTHONPATH="$PYTHONPATH:$ALTI_DIR"

src_dir="altimeter"
tests_dir="tests/unit"

if [ -z "$COVERAGE_MIN_PERCENTAGE" ]; then
    COVERAGE_MIN_PERCENTAGE=60
fi

echo "Running tests in $tests_dir against $src_dir and any doctests in $src_dir"
pytest --ignore=altimeter/qj/alembic/env.py --cov="$src_dir" --cov-report=term-missing --cov-fail-under=${COVERAGE_MIN_PERCENTAGE} --cov-branch --doctest-modules "$src_dir" "$tests_dir"
