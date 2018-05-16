#!/bin/bash
set -ex

rm -rf bin/
docker build --rm -t pgtobq:build .
docker run --rm -v $PWD/bin:/go/src/app/bin pgtobq:build

for f in bin/*; do gpg -ab $f; done
