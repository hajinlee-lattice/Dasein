#!/usr/bin/env bash

if ! [ -z "${MY_ID}" ]; then
    echo ${MY_ID} > /usr/zookeeper/data/myid
    cat /usr/zookeeper/data/myid
else
    echo "Did not provide MY_ID"
fi

/usr/zookeeper/bin/zkServer.sh start-foreground || true