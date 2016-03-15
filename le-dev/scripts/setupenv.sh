#!/bin/bash

printf "%s\n" "${HADOOP_CONF_DIR:?You must set HADOOP_CONF_DIR}"
printf "%s\n" "${WSHOME:?You must set WSHOME}"
printf "%s\n" "${CATALINA_HOME:?You must set CATALINA_HOME}"

cp $WSHOME/le-dev/hadoop/dev/capacity-scheduler.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/mapred-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/tez-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/yarn-site.xml $HADOOP_CONF_DIR

hadoop fs -mkdir -p /apps/tez || true
hadoop fs -copyFromLocal $WSHOME/le-dev/hadoop/artifacts/tez-0.6.2.tar.gz /apps/tez || true

pip install -r $WSHOME/le-dev/scripts/requirements.txt

sudo mkdir -p /etc/ledp
sudo cp $WSHOME/le-security/certificates/laca-ldap.dev.lattice.local.jks /etc/ledp

