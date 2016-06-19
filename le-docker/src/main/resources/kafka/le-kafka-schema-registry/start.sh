#!/usr/bin/env bash

sed -i "s|{{ZK_HOSTS}}|$ZK_HOSTS|g" /etc/schema-registry/schema-registry.properties
sed -i "s|{{ADVERTISE_IP}}|$ADVERTISE_IP|g" /etc/schema-registry/schema-registry.properties

while [ 1 == 1 ]; do
	/usr/bin/schema-registry-start /etc/schema-registry/schema-registry.properties
	sleep 3
done
