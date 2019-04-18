#!/bin/bash

function processErrors
{
  if [[ $? -ne 0 ]]
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
echo "You are using this java: ${JAVA_HOME}"

# Expand aliases
echo "Expanding aliases."
shopt -s expand_aliases
echo "Sourcing aliases file"
source $WSHOME/le-dev/aliases

# Top-level compile
echo "Changing dir into workspace"
cd $WSHOME

OLD_JAVA_HOME=${JAVA_HOME}
if [[ -n "${J11_HOME}" ]]; then
    export JAVA_HOME=${J11_HOME}
fi

echo "Top-level compile"
mvn -T8 -Pcheckstyle -DskipTests clean install 2> /tmp/errors.txt
processErrors

echo "Top-level shaded yarn compile"
mvn -T6 -f shaded-pom.xml -Pshaded-yarn -DskipTests clean package 2> /tmp/errors.txt
processErrors

echo "" > /tmp/errors.txt

hdfs dfs -rm -r -f /app/${LE_STACK}/$(leversion) || true
hdfs dfs -mkdir -p /app/${LE_STACK} || true
pushd ${WSHOME}/le-dataplatform
mvn -Ppkg-shaded -DskipTests package &&
echo "Deploying artifacts to hdfs ..."
hdfs dfs -copyFromLocal target/dist /app/${LE_STACK}/$(leversion)
popd

echo "deploy properties file"
cfgdpl 2> /tmp/errors.txt
processErrors

bash ${WSHOME}/le-dev/scripts/deploy_leds.sh

if [[ -d ${ANACONDA_HOME}/envs/p2 ]]; then
    source ${ANACONDA_HOME}/bin/activate p2
else
    source ${ANACONDA_HOME}/bin/activate lattice
fi

if [[ "${USE_QA_RTS}" == "true" ]]; then
    ${PYTHON} $WSHOME/le-dev/scripts/setup_zk.py --qa-source-dbs
else
    ${PYTHON} $WSHOME/le-dev/scripts/setup_zk.py
fi
source ${ANACONDA_HOME}/bin/deactivate

echo "Clean up old test tenants"
export JAVA_HOME=${OLD_JAVA_HOME}
runtest testframework -g cleanup -t GlobalAuthCleanupTestNG

echo "Success!!!"


