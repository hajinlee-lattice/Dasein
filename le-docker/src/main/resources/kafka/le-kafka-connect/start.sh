#!/usr/bin/env bash

sed -i "s|{{BOOTSTRAP_SERVERS}}|$BOOTSTRAP_SERVERS|g" /etc/kafka/connect-distributed.properties
sed -i "s|{{SR_ADDRESS}}|$SR_ADDRESS|g" /etc/kafka/connect-distributed.properties

i=0
while [ $i -le 100 ]; do

    CLASSPATH=/usr/share/java/kafka-connect-hdfs/*:/usr/share/java/kafka-connect-jdbc/*:/usr/share/java/kafka-connect-ledp/* \
    /usr/bin/connect-distributed /etc/kafka/connect-distributed.properties

    sleep 5
    i=$((i+1))
done