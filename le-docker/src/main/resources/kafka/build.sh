#!/bin/bash

# build ubuntu base image
cd le-kafka
docker build -t latticeengines/kafka . || true
cd ..

cd le-kafka-manager
docker build -t latticeengines/kafka-manager . || true
cd ..

cd le-haproxy
docker build -t latticeengines/kafka-haproxy . || true
cd ..

# create network
docker network create kafka 2> /dev/null

