#!/usr/bin/env bash

sed -i "s|{{CONSUMER_ID}}|$HOSTNAME-$(date +%s)|g" /etc/kafka-rest/kafka-rest.properties

sed -i "s|{{ZK_HOSTS}}|$ZK_HOSTS|g" /etc/kafka-rest/kafka-rest.properties
sed -i "s|{{SR_PROXY}}|$SR_PROXY|g" /etc/kafka-rest/kafka-rest.properties

while [ 1 == 1 ]; do
	/usr/bin/kafka-rest-start /etc/kafka-rest/kafka-rest.properties
	sleep 3
done

