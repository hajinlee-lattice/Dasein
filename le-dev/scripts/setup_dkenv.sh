#!/usr/bin/env bash

pushd $WSHOME/le-dev/scripts
javac Ip.java
export HOST_IP=$(java Ip)
popd