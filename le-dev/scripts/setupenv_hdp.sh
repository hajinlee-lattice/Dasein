#!/usr/bin/env bash

BOOTSTRAP_MODE=$1

if [[ "${BOOTSTRAP_MODE}" = "bootstrap" ]]; then
    echo "Bootstrapping HDP ..."

    bash $WSHOME/le-dev/scripts/stop-cluster.sh || true

    ARTIFACT_DIR=$WSHOME/le-dev/artifacts
    HDP_VERSION=2.8.5

    sudo rm -rf ${HADOOP_HOME} || true
    sudo mkdir -p ${HADOOP_HOME}
    sudo chown -R ${USER} ${HADOOP_HOME}
    if [[ ! -f "${ARTIFACT_DIR}/hadoop-${HDP_VERSION}.tar.gz" ]]; then
        echo 'downloading hadoop tar ball, this may take a long time (10 min) ...'
        aws s3 cp \
            s3://latticeengines-dev/artifacts/hadoop/common/hadoop-${HDP_VERSION}/hadoop-${HDP_VERSION}.tar.gz \
            ${ARTIFACT_DIR}/hadoop-${HDP_VERSION}.tar.gz
    fi
    rm -rf ${ARTIFACT_DIR}/hadoop-${HDP_VERSION} || true
    mkdir -p ${ARTIFACT_DIR}/hadoop-${HDP_VERSION}
    tar xzf ${ARTIFACT_DIR}/hadoop-${HDP_VERSION}.tar.gz -C ${ARTIFACT_DIR}/hadoop-${HDP_VERSION}
    if [[ -d "${ARTIFACT_DIR}/hadoop-${HDP_VERSION}/hadoop-${HDP_VERSION}" ]]; then
        cp -rf ${ARTIFACT_DIR}/hadoop-${HDP_VERSION}/hadoop-${HDP_VERSION}/* ${HADOOP_HOME}
    else
        cp -rf ${ARTIFACT_DIR}/hadoop-${HDP_VERSION}/* ${HADOOP_HOME}
    fi
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

    # mvn dependency:get -Dtransitive=false -Dartifact=com.amazonaws:aws-java-sdk-s3:1.11.433
    # mvn dependency:copy -DoutputDirectory=${HADOOP_HOME}/share/hadoop/common/lib -Dartifact=com.amazonaws:aws-java-sdk-s3:1.11.433

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

if [[ "${BOOTSTRAP_MODE}" = "bootstrap" ]]; then
    hdfs namenode -format

    bash $WSHOME/le-dev/scripts/start-cluster.sh || true
    hdfs dfsadmin -safemode leave
    if [[ "${BOOTSTRAP_MODE}" = "true" ]]; then
        for app in 'dataplatform' 'sqoop' 'eai' 'dataflow' 'dataflowapi' 'datacloud' 'workflowapi' 'scoring' 'dellebi'
        do
            hdfs dfs -mkdir -p /app/${app} || true &
        done
        wait
    fi
    hdfs dfs -mkdir /spark-history

    echo "Uploading TEZ ..."
    TEZ_VERSION=0.9.0
    if [[ ! -f "$ARTIFACT_DIR/tez-${TEZ_VERSION}.tar.gz" ]]; then
        wget --trust-server-names "https://s3.amazonaws.com/latticeengines-dev/artifacts/tez/${TEZ_VERSION}/tez-${TEZ_VERSION}.tar.gz" \
            -O $ARTIFACT_DIR/tez-${TEZ_VERSION}.tar.gz
    fi
    hdfs dfs -mkdir -p /apps/tez || true
    hdfs dfs -put -f $ARTIFACT_DIR/tez-${TEZ_VERSION}.tar.gz /apps/tez/tez-${TEZ_VERSION}.tar.gz

    bash $WSHOME/le-dev/scripts/stop-cluster.sh || true

    echo "Bootstrapping sqoop ..."
    ARTIFACT_DIR=$WSHOME/le-dev/artifacts
    SQOOP_VERSION="1.4.6.2.4.3.0-227"

    if [[ ! -f "$ARTIFACT_DIR/sqoop-${SQOOP_VERSION}.tar.gz" ]]; then
        wget https://s3.amazonaws.com/latticeengines-dev/sqoop-${SQOOP_VERSION}.tar.gz -O $ARTIFACT_DIR/sqoop-${SQOOP_VERSION}.tar.gz
    fi
    if [[ -d "${SQOOP_HOME}" ]]; then
        sudo rm -rf $SQOOP_HOME/*
    fi
    sudo mkdir -p $SQOOP_HOME
    sudo chown -R $USER $SQOOP_HOME
    tar xzf $ARTIFACT_DIR/sqoop-${SQOOP_VERSION}.tar.gz -C $SQOOP_HOME
fi
