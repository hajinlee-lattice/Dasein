#!/usr/bin/env bash

echo $MY_ID > /var/lib/zookeeper/myid

/usr/zookeeper/bin/zkServer.sh start-foreground
