#!/usr/bin/env bash

set -euf -o pipefail

export PYTHONPATH=.
ci/static_checks.sh altimeter
ci/test.sh altimeter tests/unit
sphinx-apidoc -f -o doc/source altimeter
sphinx-build doc/source doc/html -E -W
