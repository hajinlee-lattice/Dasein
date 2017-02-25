#!/usr/bin/env bash

pushd $WSHOME/le-dev/scripts
javac Ip.java
export HOST_IP=$(ifconfig en0 | awk '$1 == "inet" {print $2}')
echo "Fond host ip = ${HOST_IP}"
popd