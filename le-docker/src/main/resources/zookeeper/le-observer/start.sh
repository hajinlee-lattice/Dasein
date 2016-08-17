#!/usr/bin/env bash

cp /tmp/conf/${LE_ENVIRONMENT}/zoo.cfg /usr/zookeeper/conf/zoo.cfg

ADVERTISE_NAME=${ADVERTISE_NAME:localhost}
sed -i "s|{{ADVERTISE_NAME}}|${ADVERTISE_NAME}|g" /usr/zookeeper/conf/zoo.cfg

echo "MY_ID=${MY_ID}"
echo $MY_ID > /usr/zookeeper/data/myid

/usr/zookeeper/bin/zkServer.sh start-foreground || true
sleep 3