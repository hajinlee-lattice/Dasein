#!/usr/bin/env bash

if [ -z "${DISCOVER_SERVICE}" ];
then
    if [ -z "${ADVERTISE_IP}" ]; then
        echo "Must provide either ADVERTISE_IP or DISCOVER_SERVICE!"
        exit -1
    fi
else
    ADVERTISE_IP=""
    while [ -z "${ADVERTISE_IP}" ];
	do
	    echo "Attempt to get ZK hosts from external discover service ${DISCOVER_SERVICE}"
	    ADVERTISE_IP=`curl -X GET ${DISCOVER_SERVICE}/advertiseip`
	    echo "Got response \"${ADVERTISE_IP}\""
	    sleep 3

	    ERROR=`echo $ADVERTISE_IP | grep "DOCTYPE HTML"`
	    if [ -z "${ERROR}" ]; then
	        echo "Great! there is no error."
	    else
	        echo "Error:\n${ERROR}"
	        ADVERTISE_IP=""
	        continue;
	    fi
	    if [ -z "${ADVERTISE_IP}" ]; then
	        continue
	    fi
	done
	echo "ADVERTISE_IP=${ADVERTISE_IP}"
fi

sed -i "s|{{ZK_HOSTS}}|$ZK_HOSTS|g" /etc/schema-registry/schema-registry.properties
sed -i "s|{{ADVERTISE_IP}}|$ADVERTISE_IP|g" /etc/schema-registry/schema-registry.properties

while [ 1 == 1 ]; do
	/usr/bin/schema-registry-start /etc/schema-registry/schema-registry.properties
	sleep 3
done
