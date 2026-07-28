#!/usr/bin/env bash

set -euo pipefail

if [[ $# -gt 1 || ( $# -eq 1 && "$1" != "--upload" ) ]]; then
    echo "Usage: $0 [--upload]" >&2
    exit 2
fi

# 清理旧的构建产物，防止重复上传引发 PyPI 400 报错
rm -rf -- dist build yyds_lock.egg-info

python -m unittest discover -s tests -v
python -m build
python -m twine check dist/*

if [[ $# -eq 1 ]]; then
    python -m twine upload dist/*
else
    echo "Build verified. Re-run with --upload to publish to PyPI."
fi
