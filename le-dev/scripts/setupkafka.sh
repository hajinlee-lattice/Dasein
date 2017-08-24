#!/usr/bin/env bash

# Test for required env variables
printf "%s\n" "${CONFLUENT_HOME:?Must set CONFLUENT_HOME to your root dir for confluent platform}"
printf "%s\n" "${WSHOME:?You must set WSHOME}"

echo "Compile datafabric connect shaded jar"
pushd ${WSHOME}/le-datafabric && \
mvn -f pom-connect-shaded.xml -DskipTests package && \
popd

echo "Copy datafabric connect shaded jar to confluent home"
mkdir -p ${CONFLUENT_HOME}/connectors
cp ${WSHOME}/le-datafabric/target/le-datafabric-connect-*-shaded.jar ${CONFLUENT_HOME}/connectors
ls -al ${CONFLUENT_HOME}/connectors
