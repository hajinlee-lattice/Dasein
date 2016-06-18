#!/bin/bash

cd le-haproxy
docker build -t latticeengines/haproxy . || true
cd ..

