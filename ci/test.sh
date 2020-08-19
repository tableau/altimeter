#!/usr/bin/env bash

set -ef -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ALTI_DIR=$(dirname "$SCRIPT_DIR")
export PYTHONPATH="$PYTHONPATH:$ALTI_DIR"

src_dir="altimeter"
tests_dir="tests"

if [ -z "$COVERAGE_MIN_PERCENTAGE" ]; then
    COVERAGE_MIN_PERCENTAGE=60
fi

export DB_USER=postgres
export DB_PASSWORD=""
export DB_HOST=127.0.0.1
export DB_NAME=qj_test
psql -c "create database $DB_NAME;" -U $DB_USER
export SQLALCHEMY_URL="postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}/${DB_NAME}"
alembic -c services/qj/alembic/alembic.ini upgrade head

echo "Running tests in $tests_dir against $src_dir and any doctests in $src_dir"
pytest --ignore=altimeter/qj/alembic/env.py --cov="$src_dir" --cov-report=term-missing --cov-fail-under=${COVERAGE_MIN_PERCENTAGE} --cov-branch --doctest-modules "$src_dir" "$tests_dir"
