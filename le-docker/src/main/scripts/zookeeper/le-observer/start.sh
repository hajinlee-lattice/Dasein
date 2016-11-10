#!/usr/bin/env bash

cp /tmp/conf/${LE_ENVIRONMENT}/zoo.cfg /usr/zookeeper/conf/zoo.cfg

ADVERTISE_NAME=${ADVERTISE_NAME:=localhost}
echo ${ADVERTISE_NAME}
sed -i "s|{{ADVERTISE_NAME}}|${ADVERTISE_NAME}|g" /usr/zookeeper/conf/zoo.cfg
cat /usr/zookeeper/conf/zoo.cfg

MY_ID=${MY_ID:=4}
echo "MY_ID=${MY_ID}"
echo "${MY_ID}" > /usr/zookeeper/data/myid

/usr/zookeeper/bin/zkServer.sh start-foreground || true
sleep 3