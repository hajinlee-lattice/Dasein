#!/usr/bin/env bash

pushd $WSHOME/le-config
mvn -DskipTests clean compile
popd

UNAME=`uname`
if [[ "${UNAME}" == 'Darwin' ]]; then
    echo "You are on Mac"
    sed -i '' "s|\${LE_STACK}|${LE_STACK}|g" $WSHOME/le-config/conf/env/dev/latticeengines.properties
else
    echo "You are on ${UNAME}"
    sed -i "s|\${LE_STACK}|${LE_STACK}|g" $WSHOME/le-config/conf/env/dev/latticeengines.properties
fi