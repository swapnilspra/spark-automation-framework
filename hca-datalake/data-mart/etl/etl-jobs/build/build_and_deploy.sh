#!/usr/bin/env bash

python build.py bdist_egg --dist-dir ../dist
cd build_cdk
source ./assume_role.sh
cdk deploy
cd ..
