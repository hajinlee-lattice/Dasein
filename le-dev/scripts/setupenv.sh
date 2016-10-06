#!/bin/bash

printf "%s\n" "${HADOOP_CONF_DIR:?You must set HADOOP_CONF_DIR}"
printf "%s\n" "${WSHOME:?You must set WSHOME}"
printf "%s\n" "${LE_STACK:?You must set LE_STACK to a unique value among developers}"
printf "%s\n" "${HADOOP_NAMENODE_DATA_DIR:?You must set HADOOP_NAMENODE_DATA_DIR}"
printf "%s\n" "${HADOOP_DATANODE_DATA_DIR:?You must set HADOOP_DATANODE_DATA_DIR}"

cp $WSHOME/le-dev/hadoop/dev/capacity-scheduler.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/mapred-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/tez-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/yarn-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/kms-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/core-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/hdfs-site.xml $HADOOP_CONF_DIR
sed -i "s|[$][{]HADOOP_NAMENODE_DATA_DIR[}]|${HADOOP_NAMENODE_DATA_DIR}|" $HADOOP_CONF_DIR/hdfs-site.xml
sed -i "s|[$][{]HADOOP_DATANODE_DATA_DIR[}]|${HADOOP_DATANODE_DATA_DIR}|" $HADOOP_CONF_DIR/hdfs-site.xml

if [ ! -z "CATALINA_HOME" ]; then
    cp $WSHOME/le-dev/tomcat/dev/server.xml $CATALINA_HOME/conf/server.xml
fi

rm -rf $WSHOME/le-dev/testartifacts/certificates/cacerts || true
wget -P $WSHOME/le-dev/testartifacts/certificates http://10.41.1.10/tars/cacerts

mkdir -p $WSHOME/le-dev/hadoop/artifacts/
export TEZ_TARBALL=$WSHOME/le-dev/hadoop/artifacts/tez-0.8.2.tar.gz
hadoop fs -mkdir -p /apps/tez || true
rm -rf $TEZ_TARBALL || true
echo "downloading tez tarball from sftp"
chmod 600 $WSHOME/le-dev/sftpdevkey
scp -i $WSHOME/le-dev/sftpdevkey sftpdev@10.41.1.31:/artifactory/tez-0.8.2.tar.gz $TEZ_TARBALL
hdfs dfs -put -f $TEZ_TARBALL /apps/tez || true
rm -rf $TEZ_TARBALL || true

sudo pip install -r $WSHOME/le-dev/scripts/requirements.txt || true
pip install -r $WSHOME/le-dev/scripts/requirements.txt || true

sudo mkdir -p /etc/ledp
sudo cp $WSHOME/le-security/certificates/ledp_keystore.jks /etc/ledp

sudo mkdir -p /var/log/scoring/mapper || true
sudo chmod a+w /var/log/scoring/mapper

sudo mkdir -p /var/cache/scoringapi || true
sudo chmod a+w /var/cache/scoringapi

existing=$(hadoop key list | grep master)
if [ -z existing ]; then
    hadoop key create master
else
    echo "Master key already installed"
fi
