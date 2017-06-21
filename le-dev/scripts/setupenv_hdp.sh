#!/usr/bin/env bash

BOOTSTRAP_MODE=$1

if [ "${BOOTSTRAP_MODE}" = "bootstrap" ]; then
    echo "Bootstrapping HDP ..."

    bash $WSHOME/le-dev/scripts/stop-cluster.sh || true

    ARTIFACT_DIR=$WSHOME/le-dev/artifacts

    sudo rm -rf $HADOOP_HOME || true
    sudo mkdir -p $HADOOP_HOME
    sudo chown -R $USER $HADOOP_HOME
    if [ ! -f ""$ARTIFACT_DIR/hadoop-2.7.1.2.4.3.0-227.tar.gz ]; then
        wget https://s3.amazonaws.com/latticeengines-dev/hadoop-2.7.1.2.4.3.0-227.tar.gz -O $ARTIFACT_DIR/hadoop-2.7.1.2.4.3.0-227.tar.gz
    fi
    tar xzf $ARTIFACT_DIR/hadoop-2.7.1.2.4.3.0-227.tar.gz -C $HADOOP_HOME

    UNAME=`uname`
    if [[ "${UNAME}" == 'Darwin' ]]; then
        echo "You are on Mac"
        ln -s $HADOOP_HOME/lib/native-mac $HADOOP_HOME/lib/native
    else
        echo "You are on ${UNAME}"
        ln -s $HADOOP_HOME/lib/native-linux $HADOOP_HOME/lib/native
    fi

    mkdir -p $HADOOP_NAMENODE_DATA_DIR
    mkdir -p $HADOOP_DATANODE_DATA_DIR

    sudo mkdir -p /opt/java
    sudo rm -f /opt/java/default || true
    sudo ln -s $JAVA_HOME /opt/java/default
	
fi

echo "Configuring HDP ..."
cp $WSHOME/le-dev/hadoop/dev/capacity-scheduler.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/mapred-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/tez-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/yarn-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/kms-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/core-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/dev/hdfs-site.xml $HADOOP_CONF_DIR
sed -i".orig" "s|[$][{]HADOOP_NAMENODE_DATA_DIR[}]|${HADOOP_NAMENODE_DATA_DIR}|" $HADOOP_CONF_DIR/hdfs-site.xml
sed -i".orig" "s|[$][{]HADOOP_DATANODE_DATA_DIR[}]|${HADOOP_DATANODE_DATA_DIR}|" $HADOOP_CONF_DIR/hdfs-site.xml

if [ "${BOOTSTRAP_MODE}" = "bootstrap" ]; then
    hdfs namenode -format
    
    echo "Bootstrapping sqoop ..."
    ARTIFACT_DIR=$WSHOME/le-dev/artifacts

    if [ ! -f "$ARTIFACT_DIR/sqoop-1.4.6.2.4.3.0-227.tar.gz" ]; then
        wget https://s3.amazonaws.com/latticeengines-dev/sqoop-1.4.6.2.4.3.0-227.tar.gz -O $ARTIFACT_DIR/sqoop-1.4.6.2.4.3.0-227.tar.gz
    fi
    rm -rf $SQOOP_HOME || true
    mkdir -p $SQOOP_HOME
    chown -R $USER $SQOOP_HOME
    tar xzf $ARTIFACT_DIR/sqoop-1.4.6.2.4.3.0-227.tar.gz -C $SQOOP_HOME
fi
