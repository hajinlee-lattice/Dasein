#!/usr/bin/env bash

function version_gt() { test "$(echo "$@" | tr " " "\n" | sort | head -n 1)" != "$1"; }

function get_mysql_command() {
  db_hostname=127.0.0.1
  db_user=root
  db_password=welcome
  if ! [ -z "$1" ]; then
      db_hostname=$1
  fi
  if ! [ -z "$2" ]; then
      db_user=$2
  fi
  if ! [ -z "$3" ]; then
      db_password=$3
  fi
  UNAME=`uname`
  MYSQL_COMMAND="mysql -h ${db_hostname} -u ${db_user} -p${db_password} --local-infile=1"
  echo $MYSQL_COMMAND
}

printf "%s\n" "${HIVE_HOME:?You must set HIVE_HOME}"
printf "%s\n" "${HADOOP_HOME:?You must set HADOOP_HOME}"

BOOTSTRAP_MODE=$1

if [[ "${BOOTSTRAP_MODE}" = "bootstrap" ]]; then
    echo "Bootstrapping Hive ..."

    HIVE_VERSION=2.3.7
    ARTIFACT_DIR="$WSHOME/le-dev/artifacts"

    if [[ ! -f "${ARTIFACT_DIR}/apache-hive-${HIVE_VERSION}-bin.tar.gz" ]]; then
        TGZ_URL="https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz"
        wget ${TGZ_URL} -O ${ARTIFACT_DIR}/apache-hive-${HIVE_VERSION}-bin.tar.gz
    fi

    if [[ -d "${ARTIFACT_DIR}/apache-hive-${HIVE_VERSION}-bin" ]]; then
        rm -rf "${ARTIFACT_DIR}/apache-hive-${HIVE_VERSION}-bin"
    fi
    tar -xzf "${ARTIFACT_DIR}/apache-hive-${HIVE_VERSION}-bin.tar.gz" -C "${ARTIFACT_DIR}"

    if [[ -d "${HIVE_HOME}" ]]; then
        sudo rm -rf "${HIVE_HOME}"
    fi
    sudo mv "${ARTIFACT_DIR}/apache-hive-${HIVE_VERSION}-bin" "${HIVE_HOME}"

    if [[ ! -f "${ARTIFACT_DIR}/mysql-connector-java-8.0.22.jar" ]]; then
        JERSEY_URL="https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.22/mysql-connector-java-8.0.22.jar"
        wget --no-check-certificate ${JERSEY_URL} -O "${ARTIFACT_DIR}/mysql-connector-java-8.0.22.jar"
    fi
    sudo cp "${ARTIFACT_DIR}/mysql-connector-java-8.0.22.jar" "${HIVE_HOME}/lib"
    sudo mkdir "${HIVE_HOME}/log"
    sudo chown ${USER} "${HIVE_HOME}/log"
fi

sudo cp "${WSHOME}/le-dev/hive/hive-site.xml" "${HIVE_HOME}/conf"
MYSQL_COMMAND=`get_mysql_command`
cat "${WSHOME}/le-dev/scripts/setupdb_hive.sql" | eval ${MYSQL_COMMAND}
RETURN=$?
if [[ ! "${RETURN}" == "0" ]]; then
  echo "Need a running MySQL server to configure hive metastore"
  exit 1
fi
${HIVE_HOME}/bin/schematool -dbType mysql -initSchema
