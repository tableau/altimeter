#!/usr/bin/env bash

set -ef -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ALTI_DIR=$(dirname "$SCRIPT_DIR")
export PYTHONPATH="$PYTHONPATH:$ALTI_DIR"

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <src_dir> <tests_dir>"
    exit 1
fi

src_dir="$1"
tests_dir="$2"

if [ -z "$COVERAGE_MIN_PERCENTAGE" ]; then
    COVERAGE_MIN_PERCENTAGE=65
fi

echo "Running tests in $tests_dir against $src_dir and any doctests in $src_dir"
pytest --cov="$src_dir" --cov-report=term-missing --cov-fail-under=${COVERAGE_MIN_PERCENTAGE} --cov-branch --doctest-modules "$src_dir" "$tests_dir"
