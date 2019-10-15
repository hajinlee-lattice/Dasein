#!/usr/bin/env bash

# Test for required env variables
printf "%s\n" "${CONFLUENT_HOME:?Must set CONFLUENT_HOME to your root dir for confluent platform}"
printf "%s\n" "${WSHOME:?You must set WSHOME}"

echo "Compile datafabric connect shaded jar"
pushd ${WSHOME}/le-datafabric-connect && \
mvn -DskipTests package && \
popd

echo "Copy datafabric connect shaded jar to confluent home"
mkdir -p ${CONFLUENT_HOME}/connectors
rm -rf ${CONFLUENT_HOME}/connectors/le-datafabric-connect-*-shaded.jar
cp ${WSHOME}/le-datafabric-connect/target/le-datafabric-connect-*-shaded.jar ${CONFLUENT_HOME}/connectors
ls -ahl ${CONFLUENT_HOME}/connectors
