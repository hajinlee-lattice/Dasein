#!/usr/bin/env bash

echo $MY_ID > /var/lib/zookeeper/myid

/usr/bin/zookeeper-server-start /etc/kafka/zookeeper.properties
