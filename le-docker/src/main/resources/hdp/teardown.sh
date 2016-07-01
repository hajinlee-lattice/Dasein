#!/usr/bin/env bash

echo "stopping container hdp ... "
docker stop hdp || true
docker rm hdp  || true