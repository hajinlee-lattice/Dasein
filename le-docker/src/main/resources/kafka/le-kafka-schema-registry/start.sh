#!/usr/bin/env bash

if [ -z "${ADVERTISE_IP}" ]; then
    if [ -z "${DISCOVER_SERVICE}" ]; then
        echo "Must provid either ADVERTISE_IP or DISCOVER_SERVICE!"
        exit -1
    fi
	while [ -z "${ADVERTISE_IP}" ];
	do
	    echo "Attempt to get advertiser ip from external discover service ${DISCOVER_SERVICE}"
	    ADVERTISE_IP=`curl -X GET ${DISCOVER_SERVICE}/advertiseip`
	    echo "Got response \"${ADVERTISE_IP}\""
	    sleep 3

	    ERROR=`echo $ADVERTISE_IP | grep "500 Internal Server Error"`
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
