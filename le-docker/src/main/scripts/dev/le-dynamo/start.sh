#!/usr/bin/env bash

java -Djava.library.path=${DYNAMO_HOME}/DynamoDBLocal_lib \
    -jar ${DYNAMO_HOME}/DynamoDBLocal.jar \
    -dbPath ${DYNAMO_HOME} \
    -sharedDb 2>&1