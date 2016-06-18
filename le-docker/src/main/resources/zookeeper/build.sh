#!/usr/bin/env bash

cd le-zk
docker build -t latticeengines/zookeeper . || true
cd ..
