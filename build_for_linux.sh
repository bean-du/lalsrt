#!/usr/bin/env bash

set -x

export CGO_ENABLED=0
export GOOS=linux
export GOARCH=amd64
./build.sh
