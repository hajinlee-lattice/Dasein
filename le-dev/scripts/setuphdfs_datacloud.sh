#!/usr/bin/env bash

SOURCE_ROOT="/Pods/Default/Services/PropData/Sources"
LOCAL_DIR="${WSHOME}/le-dev/testartifacts/DataCloudSources"

hdfs dfs -rm -r -f ${SOURCE_ROOT}
hdfs dfs -mkdir -p ${SOURCE_ROOT}
echo "Uploading data cloud sources ... "
hdfs dfs -copyFromLocal ${LOCAL_DIR}/* ${SOURCE_ROOT}
hdfs dfs -ls -R ${SOURCE_ROOT}
