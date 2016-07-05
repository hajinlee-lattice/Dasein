#!/usr/bin/env bash

pushd $WSHOME/le-config
mvn -DskipTests clean compile
popd

sed -i "s|\${LE_STACK}|${LE_STACK}|g" $WSHOME/le-config/conf/env/dev/latticeengines.properties