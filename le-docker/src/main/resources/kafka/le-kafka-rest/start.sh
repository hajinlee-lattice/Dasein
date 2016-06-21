#!/usr/bin/env bash


if [ -z "${DISCOVER_SERVICE}" ];
then
    if [ -z "${ZK_HOSTS}" ]; then
        echo "Must provide either ZK_HOSTS or DISCOVER_SERVICE!"
        exit -1
    fi
else
    if [ -z "${KAFKA_CLUSTER_NAME}" ]; then
        echo "Must provide KAFKA_CLUSTER_NAME"
        exit -1
    fi

    if [ "${RETRIEVE_INTERNAL_ADDR}" == "true" ]; then

        SERVICE_ADDR=""
        while [ -z "${SERVICE_ADDR}" ];
        do
            echo "Attempt to get quorum from external discover service ${DISCOVER_SERVICE}/internal_addr"
            SERVICE_ADDR=`curl -X GET ${DISCOVER_SERVICE}/internal_addr`
            echo "Got response \"${SERVICE_ADDR}\""
            sleep 3

            ERROR=`echo $SERVICE_ADDR | grep "500 Internal Server Error"`
            if [ -z "${ERROR}" ]; then
                echo "Great! there is no error."
            else
                echo "Error:\n${ERROR}"
                SERVICE_ADDR=""
                continue;
            fi
            if [ -z "${SERVICE_ADDR}" ]; then
                continue
            fi
        done
        echo "SERVICE_ADDR=${SERVICE_ADDR}"

    else
        SERVICE_ADDR=$DISCOVER_SERVICE
    fi

    echo using service address $SERVICE_ADDR

    while [ -z "${ZK_HOSTS}" ];
	do
	    echo "Attempt to get ZK hosts from external discover service ${SERVICE_ADDR}/quorums/${KAFKA_CLUSTER_NAME}/zkhosts"
	    ZK_HOSTS=`curl -X GET ${SERVICE_ADDR}/quorums/${KAFKA_CLUSTER_NAME}/zkhosts`
	    echo "Got response \"${ZK_HOSTS}\""
	    sleep 3

	    ERROR=`echo $ZK_HOSTS | grep "DOCTYPE HTML"`
	    if [ -z "${ERROR}" ]; then
	        echo "Great! there is no error."
	    else
	        echo "Error:\n${ERROR}"
	        ZK_HOSTS=""
	        continue;
	    fi
	    if [ -z "${ZK_HOSTS}" ]; then
	        continue
	    fi
	done
	echo "ZK_HOSTS=${ZK_HOSTS}"

fi

sed -i "s|{{CONSUMER_ID}}|$HOSTNAME-$(date +%s)|g" /etc/kafka-rest/kafka-rest.properties
sed -i "s|{{ZK_HOSTS}}|$ZK_HOSTS|g" /etc/kafka-rest/kafka-rest.properties
sed -i "s|{{SR_PROXY}}|$SR_PROXY|g" /etc/kafka-rest/kafka-rest.properties

while [ 1 == 1 ]; do
	/usr/bin/kafka-rest-start /etc/kafka-rest/kafka-rest.properties
	sleep 3
done

