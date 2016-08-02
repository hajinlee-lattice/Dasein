#!/bin/bash

export LE_PROPDIR=$WSHOME/le-config/conf/env/dev

PRODUCT=$1
TEST_TYPE=$2
TEST=$3
TESTGROUP=$2
if [ $TEST_TYPE = "deployment" ]; then
    TESTGROUP=$TEST_TYPE.$PRODUCT
fi

mvn -P$TEST_TYPE -Dtest=$TEST -D$TEST_TYPE.groups=$TESTGROUP verify -DargLine=""
