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
mvn -T8 -Pgenerate -DskipTests clean install 2> /tmp/errors.txt
processErrors

echo "Top-level playmaker compile"
mvn -T6 -f playmaker-pom.xml -DskipTests clean install 2> /tmp/errors.txt
processErrors

echo "Deploying framework properties to local Hadoop"
fwkdpl 2> /tmp/errors.txt
processErrors

echo "Top-level shaded yarn compile"
mvn -T6 -f shaded-pom.xml -Pshaded-yarn -DskipTests clean package 2> /tmp/errors.txt
processErrors

for servicecmd in 'dataplatform|dpdplnobld' 'eai|eaidplnobld' 'dataflow|dfdplnobld' 'dataflowapi|dfapidplnobld' 'propdata|pddplnobld' 'dellebi|dedplnobld'
do
    service=`echo $servicecmd | cut -d \| -f 1` &&
    cmd=`echo $servicecmd | cut -d \| -f 2` &&
    echo "Deploying ${service} to local Hadoop using ${cmd}" &&
    pushd $WSHOME/le-$service &&
    eval $cmd 2> /tmp/errors.txt &&
    popd &&
    processErrors &
done
wait

for servicecmd in 'workflow|wfdblnobld' 'workflowapi|wfapidplnobld' 'scoring|scoringdplnobld' 'swlib|swlibdpl' 'microservice|microservicedplnobld'
do
    service=`echo $servicecmd | cut -d \| -f 1` &&
    cmd=`echo $servicecmd | cut -d \| -f 2` &&
    echo "Deploying ${service} to local Hadoop using ${cmd}" &&
    pushd $WSHOME/le-$service &&
    eval $cmd 2> /tmp/errors.txt &&
    popd &&
    processErrors &
done
wait

echo "Success!!!"



