#!/bin/bash

# colorize output
RED="$(tput setaf 1)"
YELLOW="$(tput setaf 3)"
GREEN="$(tput setaf 2)"
OFF="$(tput sgr0)"

function processErrors() {
  if [[ $? -ne 0 ]]; then
    echo "[${RED}ERROR${OFF}]"
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

source "${WSHOME}/le-dev/scripts/check_aws_creds_expiration.sh"
check_aws_creds_expiration

if [[ ! -d "/var/log/ledp" ]]; then
  sudo mkdir -p /var/log/ledp
  sudo chmod 777 /var/log/ledp
fi

# Expand aliases
echo "Expanding aliases."
shopt -s expand_aliases
echo "Sourcing aliases file"
source "${WSHOME}/le-dev/aliases"

# Top-level compile
echo "Changing dir into workspace"
cd "${WSHOME}" || exit

OLD_JAVA_HOME="${JAVA_HOME}"
if [[ -n "${J11_HOME}" ]]; then
  export JAVA_HOME="${J11_HOME}"
fi

echo "Top-level compile"
mvn -T8 -Pcheckstyle -DskipTests clean install 2>/tmp/errors.txt
processErrors

echo "Top-level shaded yarn compile"
mvn -T6 -f shaded-pom.xml -Pshaded-yarn -DskipTests clean package 2>/tmp/errors.txt
processErrors

echo "" >/tmp/errors.txt

hdfs dfs -rm -r -f "/app/${LE_STACK}/$(leversion)" || true
hdfs dfs -mkdir -p "/app/${LE_STACK}" || true
pushd "${WSHOME}/le-dataplatform" || exit
mvn -Ppkg-shaded -DskipTests package &&
  echo "Deploying artifacts to hdfs ..."
hdfs dfs -copyFromLocal target/dist "/app/${LE_STACK}/$(leversion)"
popd || exit

echo "deploy properties file"
cfgdpl 2>/tmp/errors.txt
processErrors

if [[ -n $("${ANACONDA_HOME}/bin/conda" env list | grep p2) ]]; then
  source "${ANACONDA_HOME}/bin/activate" p2
else
  source "${ANACONDA_HOME}/bin/activate" lattice
fi

if [[ "${USE_QA_RTS}" == "true" ]]; then
  ${PYTHON} "$WSHOME/le-dev/scripts/setup_zk.py" --qa-source-dbs
else
  ${PYTHON} "$WSHOME/le-dev/scripts/setup_zk.py"
fi
source "${ANACONDA_HOME}/bin/deactivate"

bash "${WSHOME}/le-dev/scripts/deploy_leds.sh"

echo "Clean up old test tenants"
export JAVA_HOME="${OLD_JAVA_HOME}"
runtest testframework -g cleanup -t GlobalAuthCleanupTestNG

echo "${GREEN}Success!!!${OFF}"
