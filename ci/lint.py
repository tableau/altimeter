#!/usr/bin/env python3
"""Run pylint against a provided set of python files."""
import argparse
import glob
import logging
from typing import List
import sys

from pylint import lint

LOGGER = logging.getLogger(__name__)


def lint_py(src_dirs: List[str], min_score: int) -> int:
    errors = []
    for src_dir in src_dirs:
        LOGGER.info("=" * 78)
        LOGGER.info("Running pylint against %s with minimum score %d", src_dir, min_score)
        LOGGER.info("=" * 78)
        files = glob.glob("{}/**/*.py".format(src_dir), recursive=True)
        errors += lint_files(files, min_score)
    if errors:
        LOGGER.error("=" * 78)
        LOGGER.error("pylint with minimum score %s failed:", min_score)
        for error in errors:
            LOGGER.error(error)
        LOGGER.error("=" * 78)
        return 1
    LOGGER.info("=" * 78)
    LOGGER.info("pylint against %s with minimum score %d completed successfully", src_dirs, min_score)
    LOGGER.info("=" * 78)
    return 0


def lint_files(files: List[str], min_score: int) -> List[str]:
    """Run pylint against a provided set of python files. Return a list of strings indicating
    filepaths and scores for any which do not meet min_score."""
    errors = []
    for py_file in files:
        score = lint_file(py_file)
        if score < min_score:
            error = "pylint {} yielded score {}, < {}".format(py_file, score, min_score)
            LOGGER.error(error)
            errors.append(error)
    return errors


def lint_file(py_file: str) -> float:
    """Run pylint against a file returning a score"""
    run = lint.Run([py_file], do_exit=False)
    score = run.linter.stats.get("global_note", 10)
    return float(score)


def check_range(n: int) -> int:
    try:
        value = int(n)
        if 0 <= value <= 10:
            return value
    except ValueError:
        pass
    raise argparse.ArgumentTypeError(f"{n} should be an integer between 0 and 10")


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("src_dirs", type=str, nargs="+", help="Source dirs to recursively lint")
    parser.add_argument(
        "--min_score", type=check_range, help="Minimum integer score, between 0 and 10."
    )

    args_ns = parser.parse_args(argv)
    return lint_py(args_ns.src_dirs, args_ns.min_score)


if __name__ == "__main__":
    sys.exit(main())
