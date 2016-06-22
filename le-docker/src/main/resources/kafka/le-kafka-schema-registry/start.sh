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

sed -i "s|{{ZK_HOSTS}}|$ZK_HOSTS|g" /etc/schema-registry/schema-registry.properties
sed -i "s|{{ADVERTISE_IP}}|$ADVERTISE_IP|g" /etc/schema-registry/schema-registry.properties

while [ 1 == 1 ]; do
	/usr/bin/schema-registry-start /etc/schema-registry/schema-registry.properties
	sleep 3
done
