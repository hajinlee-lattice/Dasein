#!/bin/bash

lib=$1
if [ -z "${lib}" ]; then
    echo "You must specify a library to be uploaded"
    exit -1
fi

# deploy a lib jar to hdfs
version=$(cat ${WSHOME}/le-parent/pom.xml | grep \<version\> | head -n 1 | cut -d \< -f 2 | cut -d \> -f 2)
stack="${LE_STACK}"
hdfs_path="/app/${stack}/${version}/${lib}/lib"

jar="le-${lib}*shaded.jar"
local_path="${WSHOME}/le-${lib}/target/${jar}"

echo "uploading ${local_path} to ${hdfs_path}"

hdfs dfs -mkdir -p ${hdfs_path}
hdfs dfs -put -f ${local_path} ${hdfs_path}
