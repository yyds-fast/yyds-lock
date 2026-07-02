#!/bin/bash

# 清理旧的构建产物，防止重复上传引发 PyPI 400 报错
rm -rf dist build yyds_lock.egg-info

python -m build
