#!/usr/bin/env bash

BOOTSTRAP_MODE=$1

if [ "${BOOTSTRAP_MODE}" = "bootstrap" ]; then
    echo "Bootstrapping HDP ..."

    bash $WSHOME/le-dev/scripts/stop-cluster.sh || true

    ARTIFACT_DIR=$WSHOME/le-dev/artifacts
    HDP_VERSION="2.7.1.2.4.3.0-227"

    sudo rm -rf ${HADOOP_HOME} || true
    sudo mkdir -p ${HADOOP_HOME}
    sudo chown -R ${USER} ${HADOOP_HOME}
    if [ ! -f "${ARTIFACT_DIR}/hadoop-${HDP_VERSION}.tar.gz" ]; then
        wget https://s3.amazonaws.com/latticeengines-dev/hadoop-${HDP_VERSION}.tar.gz -O ${ARTIFACT_DIR}/hadoop-${HDP_VERSION}.tar.gz
    fi
    rm -rf ${ARTIFACT_DIR}/hadoop-${HDP_VERSION} || true
    tar xzf ${ARTIFACT_DIR}/hadoop-${HDP_VERSION}.tar.gz -C ${ARTIFACT_DIR}
    cp -rf ${ARTIFACT_DIR}/hadoop-${HDP_VERSION}/* ${HADOOP_HOME}
    rm -rf ${ARTIFACT_DIR}/hadoop-${HDP_VERSION}

    rm -rf ${HADOOP_HOME}/lib/native || true
    UNAME=`uname`
    if [[ "${UNAME}" == 'Darwin' ]]; then
        echo "You are on Mac"
        ln -s ${HADOOP_HOME}/lib/native-mac ${HADOOP_HOME}/lib/native
    else
        echo "You are on ${UNAME}"
        ln -s ${HADOOP_HOME}/lib/native-linux ${HADOOP_HOME}/lib/native
    fi

    mkdir -p $HADOOP_NAMENODE_DATA_DIR
    mkdir -p $HADOOP_DATANODE_DATA_DIR

    sudo mkdir -p /opt/java
    sudo rm -f /opt/java/default || true
    sudo ln -s $JAVA_HOME /opt/java/default
	
fi

UNAME=`uname`
if [[ "${UNAME}" == 'Darwin' ]]; then
    CORE_SITE_XML=core-site-mac.xml
else
    CORE_SITE_XML=core-site-linux.xml
fi

echo "Configuring HDP ..."
cp $WSHOME/le-dev/hadoop/hadoop-env.sh $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/capacity-scheduler.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/mapred-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/tez-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/yarn-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/kms-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/${CORE_SITE_XML} $HADOOP_CONF_DIR/core-site.xml
cp $WSHOME/le-dev/hadoop/hdfs-site.xml $HADOOP_CONF_DIR
sed -i".orig" "s|[$][{]HADOOP_NAMENODE_DATA_DIR[}]|${HADOOP_NAMENODE_DATA_DIR}|" $HADOOP_CONF_DIR/hdfs-site.xml
sed -i".orig" "s|[$][{]HADOOP_DATANODE_DATA_DIR[}]|${HADOOP_DATANODE_DATA_DIR}|" $HADOOP_CONF_DIR/hdfs-site.xml

if [ "${BOOTSTRAP_MODE}" = "bootstrap" ]; then
    hdfs namenode -format
    
    echo "Bootstrapping sqoop ..."
    ARTIFACT_DIR=$WSHOME/le-dev/artifacts
    SQOOP_VERSION="1.4.6.2.4.3.0-227"

    if [ ! -f "$ARTIFACT_DIR/sqoop-${SQOOP_VERSION}.tar.gz" ]; then
        wget https://s3.amazonaws.com/latticeengines-dev/sqoop-${SQOOP_VERSION}.tar.gz -O $ARTIFACT_DIR/sqoop-${SQOOP_VERSION}.tar.gz
    fi
    rm -rf $SQOOP_HOME || true
    mkdir -p $SQOOP_HOME
    chown -R $USER $SQOOP_HOME
    tar xzf $ARTIFACT_DIR/sqoop-${SQOOP_VERSION}.tar.gz -C $SQOOP_HOME
fi
