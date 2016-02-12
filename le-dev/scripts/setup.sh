#!/bin/bash

function processErrors
{
  if [ $? -ne 0 ]
  then
      echo "Error!"
      cat /tmp/errors.txt
      exit 1
  fi
}

# Test for required env variables
printf "%s\n" "${WSHOME:?You must set WSHOME}"

# Expand aliases
echo "Expanding aliases."
shopt -s expand_aliases
echo "Sourcing aliases file"
source $WSHOME/le-dev/aliases

# Top-level compile
echo "Changing dir into workspace"
cd $WSHOME
echo "Top-level compile"
mvn -Pgenerate -DskipTests clean install 2> /tmp/errors.txt
processErrors

echo "Deploying framework properties to local Hadoop"
fwkdpl 2> /tmp/errors.txt
processErrors

echo "Deploying dataplatform to local Hadoop"
cd $WSHOME/le-dataplatform && dpdpl 2> /tmp/errors.txt
processErrors

echo "Deploying eai to local Hadoop"
cd $WSHOME/le-eai && eaidpl 2> /tmp/errors.txt
processErrors

echo "Deploying dataflow to local Hadoop"
cd $WSHOME/le-dataflow && dfdpl 2> /tmp/errors.txt
processErrors

echo "Deploying dataflowapi to local Hadoop"
cd $WSHOME/le-dataflowapi && dfapidpl 2> /tmp/errors.txt
processErrors

echo "Deploying workflow to local Hadoop"
cd $WSHOME/le-workflow && wfdpl 2> /tmp/errors.txt
processErrors

echo "Deploying workflowapi to local Hadoop"
cd $WSHOME/le-workflowapi && wfapidpl 2> /tmp/errors.txt
processErrors

echo "Deploying scoring to local Hadoop"
cd $WSHOME/le-scoring && scoringdpl 2> /tmp/errors.txt
processErrors

echo "Deploying swlib to local Hadoop"
cd $WSHOME/le-swlib && swlibdpl 2> /tmp/errors.txt
processErrors

echo "Deploying microservices to local jetty"
cd $WSHOME/le-microservice && microservicedplnobld 2> /tmp/errors.txt
processErrors

echo "Success!!!"



