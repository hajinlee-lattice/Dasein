#!/usr/bin/env bash

function valid_ip()
{
    local  ip=$1
    local  stat=1

    if [[ $ip =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
        OIFS=$IFS
        IFS='.'
        ip=($ip)
        IFS=$OIFS
        [[ ${ip[0]} -le 255 && ${ip[1]} -le 255 \
            && ${ip[2]} -le 255 && ${ip[3]} -le 255 ]]
        stat=$?
    fi
    return $stat
}

# on AWS we save public and private ip on the EC2 instance

if [ -z "${ADVERTISE_IP}" ]; then
    if [ -f /etc/externaladdr.txt ]; then
        ADVERTISE_IP=`cat /etc/externaladdr.txt`
    fi
fi

if [ -z "${ADVERTISE_IP}" ]; then
    if [ -f /etc/internaladdr.txt ]; then
        ADVERTISE_IP=`cat /etc/internaladdr.txt`
    fi
fi

# on local we pass in host name, and here we resolve it to ip
if ! valid_ip ${ADVERTISE_IP} ; then
    ADVERTISE_IP=$(dig +short ${ADVERTISE_IP})
fi

echo "ADVERTISE_IP=${ADVERTISE_IP}"

sed -i "s|{{ZK_HOSTS}}|$ZK_HOSTS|g" /etc/schema-registry/schema-registry.properties
sed -i "s|{{ADVERTISE_IP}}|$ADVERTISE_IP|g" /etc/schema-registry/schema-registry.properties
sed -i "s|{{ZK_NAMESPACE}}|${ZK_NAMESPACE:=schema_registry}|g" /etc/schema-registry/schema-registry.properties

/usr/bin/schema-registry-start /etc/schema-registry/schema-registry.properties
