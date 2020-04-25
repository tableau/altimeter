#!/usr/bin/env bash

set -ef -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ALTI_DIR=$(dirname "$SCRIPT_DIR")
export PYTHONPATH="$PYTHONPATH:$ALTI_DIR"

if [ -z "$PYLINT_MIN_SCORE" ]; then
    PYLINT_MIN_SCORE=9
fi
dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
black_cmd="black -l 100 --check altimeter bin"
lint_cmd="$dir/lint.py altimeter bin --min_score ${PYLINT_MIN_SCORE}"
mypy_cmd="mypy --ignore-missing-imports --disallow-untyped-defs altimeter bin"
vulture_cmd="vulture --ignore-names lambda_handler altimeter bin"
pyflakes_cmd="pyflakes altimeter bin"

echo "Running '$black_cmd' ..."
$black_cmd
if [ $? -ne 0 ]; then
    echo "'$black_cmd' failed.  Run this black command without --check on the codebase to fix."
    exit 1
fi

echo "Running '$lint_cmd' ..."
$lint_cmd
lint_status=$?

echo "Running '$mypy_cmd' ..."
$mypy_cmd
mypy_status=$?

echo "Running '$vulture_cmd' ..."
$vulture_cmd
vulture_status=$?

echo "Running '$pyflakes_cmd' ..."
$pyflakes_cmd
pyflakes_status=$?

[ $lint_status -eq 0 ] || echo "ERROR: '$lint_cmd' returned non-0. See output above for details."
[ $mypy_status -eq 0 ] || echo "ERROR: '$mypy_cmd' returned non-0. See output above for details."
[ $vulture_status -eq 0 ] || echo "ERROR: '$vulture_cmd' returned non-0. See output above for details."
[ $pyflakes_status -eq 0 ] || echo "ERROR: '$pyflakes_cmd' returned non-0. See output above for details."

[ $lint_status -eq 0 ] && [ $mypy_status -eq 0 ] && [ $vulture_status -eq 0 ] && [ $pyflakes_status -eq 0 ] && exit 0
exit 1
