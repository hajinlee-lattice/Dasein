#!/bin/bash

cd le-kafka
docker build -t latticeengines/kafka . || true
cd ..

cd le-kafka-manager
docker build -t latticeengines/kafka-manager . || true
cd ..

cd ../common/le-haproxy
docker build -t latticeengines/haproxy . || true
cd ..

