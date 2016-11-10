#!/usr/bin/env bash

for service in 'kafka' 'zookeeper' 'redis' 'consul'
do
    pushd ${service}
    bash teardown.sh
    popd
done
