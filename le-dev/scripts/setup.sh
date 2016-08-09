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
printf "%s\n" "${LE_STACK:?You must set LE_STACK to a unique value among developers}"

# Expand aliases
echo "Expanding aliases."
shopt -s expand_aliases
echo "Sourcing aliases file"
source $WSHOME/le-dev/aliases

## Top-level compile
#echo "Changing dir into workspace"
#cd $WSHOME
#echo "Top-level compile"
#mvn -T8 -DskipTests clean install 2> /tmp/errors.txt
#processErrors
#
#echo "Top-level playmaker compile"
#mvn -T6 -f playmaker-pom.xml -DskipTests clean install 2> /tmp/errors.txt
#processErrors

echo "Deploying latticeengines properties to local Hadoop"
cfgdpl 2> /tmp/errors.txt
processErrors

echo "Top-level shaded yarn compile"
mvn -T6 -f shaded-pom.xml -Pshaded-yarn -DskipTests clean package 2> /tmp/errors.txt
processErrors

echo "" > /tmp/errors.txt

for servicecmd in 'dataplatform|dpdplnobld' 'eai|eaidplnobld' 'dataflow|dfdplnobld' 'dataflowapi|dfapidplnobld' 'propdata|pddplnobld' 'dellebi|dedplnobld'
do
    service=`echo $servicecmd | cut -d \| -f 1` &&
    cmd=`echo $servicecmd | cut -d \| -f 2` &&
    echo "Deploying ${service} to local Hadoop using ${cmd}" &&
    pushd $WSHOME/le-$service &&
    eval $cmd 2>> /tmp/errors.txt &&
    popd &
done
wait

for servicecmd in 'workflow|wfdplnobld' 'workflowapi|wfapidplnobld' 'scoring|scoringdplnobld' 'swlib|swlibdpl' 'microservice|microservicedplnobld'
do
    service=`echo $servicecmd | cut -d \| -f 1` &&
    cmd=`echo $servicecmd | cut -d \| -f 2` &&
    echo "Deploying ${service} to local Hadoop using ${cmd}" &&
    pushd $WSHOME/le-$service &&
    eval $cmd 2>> /tmp/errors.txt &&
    popd &
done
wait

UNAME=`uname`
if [[ "${UNAME}" == 'Darwin' ]]; then
    echo "You are on Mac"
    sed -i '' "/INFO fs.TrashPolicyDefault/d" /tmp/errors.txt
    sed -i '' "/WARN util.NativeCodeLoader/d" /tmp/errors.txt
else
    echo "You are on ${UNAME}"
    sed -i "/INFO fs.TrashPolicyDefault/d" /tmp/errors.txt
    sed -i "/WARN util.NativeCodeLoader/d" /tmp/errors.txt
fi

if [ ! -z "$(cat /tmp/errors.txt)" ]
then
    echo "Error!"
    cat /tmp/errors.txt
    exit -1
fi

echo "deploy properties file"
cfgdpl 2> /tmp/errors.txt
processErrors

echo "Rebuild admin war"
pushd $WSHOME/le-admin; mvn -DskipTests clean install; popd;
processErrors

echo "Success!!!"
