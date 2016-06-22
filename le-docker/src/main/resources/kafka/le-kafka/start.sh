#!/usr/bin/env bash

if [ -z "${ADVERTISE_IP}" ]; then
    if [ -f /etc/internaladdr.txt ]; then
        ADVERTISE_IP=`cat /etc/internaladdr.txt`
    fi
fi

if [ -z "${ADVERTISE_IP2}" ]; then
    if [ -f /etc/externaladdr.txt ]; then
        ADVERTISE_IP=`cat /etc/externaladdr.txt`
    fi
fi

echo "ADVERTISE_IP=${ADVERTISE_IP}"

if [ -z "${ZK_HOSTS}" ]; then
    echo "Must provide either ZK_HOSTS or DISCOVER_SERVICE!"
    exit -1
fi

echo "ZK_HOSTS=${ZK_HOSTS}"

if [ -z "${BROKER_PORT}" ]; then
    BROKER_PORT=9092
fi

sed -i "s|{{BROKER_PORT}}|$BROKER_PORT|g" /etc/kafka/server.properties
sed -i "s|{{ADVERTISE_IP}}|$ADVERTISE_IP|g" /etc/kafka/server.properties
sed -i "s|{{ZK_HOSTS}}|$ZK_HOSTS|g" /etc/kafka/server.properties


JMX_PORT=9199 /usr/bin/kafka-server-start /etc/kafka/server.properties
