#!/usr/bin/env bash

sed -i "s|{{CONSUMER_ID}}|$HOSTNAME-$(date +%s)|g" /etc/kafka-rest/kafka-rest.properties
sed -i "s|{{ZK_HOSTS}}|$ZK_HOSTS|g" /etc/kafka-rest/kafka-rest.properties
sed -i "s|{{SR_ADDRESS}}|$SR_ADDRESS|g" /etc/kafka-rest/kafka-rest.properties

i=0
while [ $i -le 100 ]; do
    /usr/bin/kafka-rest-start /etc/kafka-rest/kafka-rest.properties

    sleep 5
    i=$((i+1))
done
