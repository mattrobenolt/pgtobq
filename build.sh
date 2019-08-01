#!/bin/bash
set -ex

rm -rf bin/
docker build --pull --rm -t pgtobq:build .
docker run --rm -v $PWD/bin:/usr/src/pgtobq/bin pgtobq:build

for f in bin/*; do gpg -ab $f; done
