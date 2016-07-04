#!/usr/bin/env bash

for service in 'consul' 'redis' 'zookeeper' 'kafka'
do
    pushd ${service}
    bash bootstrap.sh
    popd
done