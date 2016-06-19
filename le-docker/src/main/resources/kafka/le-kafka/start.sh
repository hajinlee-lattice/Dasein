#!/usr/bin/env bash

if [ -z ADVERTISE_IP ]; then
	ADVERTISE_IP=$HOSTNAME
fi

sed -i "s|{{ZK_HOSTS}}|$ZK_HOSTS|g" /etc/kafka/server.properties
sed -i "s|{{ADVERTISE_IP}}|$ADVERTISE_IP|g" /etc/kafka/server.properties

JMX_PORT=9199 /usr/bin/kafka-server-start /etc/kafka/server.properties
