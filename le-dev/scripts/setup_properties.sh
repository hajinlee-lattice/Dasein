#!/usr/bin/env bash

rm -r -f /tmp/leprop || true
mkdir -p /tmp/leprop || true
cp $WSHOME/le-config/conf/env/devcluster/latticeengines.properties /tmp/leprop/latticeengines.properties

touch /tmp/leprop/local.profile
echo "LE_STACK=${LE_STACK}" > /tmp/leprop/local.profile
echo "LE_ENVIRONMENT=${LE_ENVIRONMENT:=local}" >> /tmp/leprop/local.profile
echo "LE_CLIENT_ADDRESS=${LE_CLIENT_ADDRESS:=localhost}" >> /tmp/leprop/local.profile

PYTHON=${PYTHON:=python}

$PYTHON $WSHOME/le-config/src/main/python/replace_token.py /tmp/leprop /tmp/leprop/local.profile

# Expand aliases
echo "Expanding aliases."
shopt -s expand_aliases
echo "Sourcing aliases file"
source $WSHOME/le-dev/aliases

hdfs dfs -put -f /tmp/leprop/latticeengines.properties webhdfs://bodcdevvhort148.lattice.local:50070/app/${LE_STACK}/$(leversion)/conf/
