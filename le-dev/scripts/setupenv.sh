#!/bin/bash

printf "%s\n" "${HADOOP_CONF_DIR:?You must set HADOOP_CONF_DIR}"
printf "%s\n" "${WSHOME:?You must set WSHOME}"

cp $WSHOME/le-dev/hadoop/dev/capacity-scheduler.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/mapred-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/tez-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/yarn-site.xml $HADOOP_CONF_DIR

mkdir -p $WSHOME/le-dev/hadoop/artifacts/
export TEZ_TARBALL=$WSHOME/le-dev/hadoop/artifacts/tez-0.8.2.tar.gz
hadoop fs -mkdir -p /apps/tez || true
rm -rf $TEZ_TARBALL || true
echo "downloading tez tarball from sftp"
chmod 600 $WSHOME/le-dev/sftpdevkey
scp -i $WSHOME/le-dev/sftpdevkey sftpdev@10.41.1.31:/artifactory/tez-0.8.2.tar.gz $TEZ_TARBALL
hadoop fs -copyFromLocal $TEZ_TARBALL /apps/tez || true
rm -rf $TEZ_TARBALL || true

sudo pip install -r $WSHOME/le-dev/scripts/requirements.txt

sudo mkdir -p /etc/ledp
sudo cp $WSHOME/le-security/certificates/laca-ldap.dev.lattice.local.jks /etc/ledp

sudo mkdir -p /var/log/scoring/mapper || true
sudo chmod a+w /var/log/scoring/mapper

sudo mkdir -p /var/cache/scoringapi || true
sudo chmod a+w /var/cache/scoringapi

