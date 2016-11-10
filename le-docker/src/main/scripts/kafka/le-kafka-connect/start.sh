#!/usr/bin/env bash

sed -i "s|{{GROUP_ID}}|${GROUP_ID:=kc}|g" /etc/kafka/connect-distributed.properties
sed -i "s|{{BOOTSTRAP_SERVERS}}|$BOOTSTRAP_SERVERS|g" /etc/kafka/connect-distributed.properties
sed -i "s|{{SR_ADDRESS}}|$SR_ADDRESS|g" /etc/kafka/connect-distributed.properties

CLASSPATH=/usr/share/java/kafka-connect-hdfs/*:/usr/share/java/kafka-connect-jdbc/*:/usr/share/java/kafka-connect-ledp/* \
/usr/bin/connect-distributed /etc/kafka/connect-distributed.properties