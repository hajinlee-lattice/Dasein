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


PYTHON=${PYTHON:=python}

# Test for required env variables
printf "%s\n" "${WSHOME:?You must set WSHOME}"
printf "%s\n" "${LE_STACK:?You must set LE_STACK to a unique value among developers}"
echo "You are using this python: ${PYTHON}"

# Expand aliases
echo "Expanding aliases."
shopt -s expand_aliases
echo "Sourcing aliases file"
source $WSHOME/le-dev/aliases

# Top-level compile
echo "Changing dir into workspace"
cd $WSHOME
echo "Top-level compile"
mvn -T8 -DskipTests clean install 2> /tmp/errors.txt
processErrors

echo "Top-level shaded yarn compile"
mvn -T6 -f shaded-pom.xml -Pshaded-yarn -DskipTests clean package 2> /tmp/errors.txt
processErrors

echo "" > /tmp/errors.txt

for servicecmd in 'dataplatform|dpdplnobld' 'sqoop|sqdplnobld' 'eai|eaidplnobld' 'dataflow|dfdplnobld' 'dataflowapi|dfapidplnobld'
do
    service=`echo $servicecmd | cut -d \| -f 1` &&
    cmd=`echo $servicecmd | cut -d \| -f 2` &&
    echo "Deploying ${service} to local Hadoop using ${cmd}" &&
    pushd $WSHOME/le-${service} &&
    eval $cmd 2>> /tmp/errors.txt &&
    popd &
done
wait

for servicecmd in 'datacloud|dcdplnobld' 'workflowapi|wfapidplnobld' 'scoring|scoringdplnobld' 'swlib|swlibdpl' 'dellebi|dedplnobld'
do
    service=`echo $servicecmd | cut -d \| -f 1` &&
    cmd=`echo $servicecmd | cut -d \| -f 2` &&
    echo "Deploying ${service} to local Hadoop using ${cmd}" &&
    pushd $WSHOME/le-${service} &&
    eval $cmd 2>> /tmp/errors.txt &&
    popd &
done
wait

UNAME=`uname`
if [[ "${UNAME}" == 'Darwin' ]]; then
    echo "You are on Mac"
    sed -i '' "/INFO /d" /tmp/errors.txt
    sed -i '' "/WARN util.NativeCodeLoader/d" /tmp/errors.txt
    sed -i '' "/\/app\/swlib/d" /tmp/errors.txt
else
    echo "You are on ${UNAME}"
    sed -i "/INFO /d" /tmp/errors.txt
    sed -i "/WARN util.NativeCodeLoader/d" /tmp/errors.txt
    sed -i "/\/app\/swlib/d" /tmp/errors.txt
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

if [ "${USE_QA_RTS}" == "true" ]; then
    ${PYTHON} $WSHOME/le-dev/scripts/setup_zk.py --qa-source-dbs
else
    ${PYTHON} $WSHOME/le-dev/scripts/setup_zk.py
fi

if [ "${LE_ENVIRONMENT}" = "devcluster" ]; then
    VERSION=`leversion`
    echo "copying ${VERSION} artifacts from local hadoop to devcluster"
    hdfs dfs -rm -r -f hdfs://bodcdevvhort148.lattice.local:8020/app/${LE_STACK} || true
    hdfs dfs -mkdir -p hdfs://bodcdevvhort148.lattice.local:8020/app/${LE_STACK}
    hdfs dfs -cp /app hdfs://bodcdevvhort148.lattice.local:8020/app/${LE_STACK}/${VERSION}
    hdfs dfs -ls hdfs://bodcdevvhort148.lattice.local:8020/app/${LE_STACK}
fi

echo "Clean up old test tenants"
runtest testframework -g cleanup -t GlobalAuthCleanupTestNG

echo "Success!!!"
