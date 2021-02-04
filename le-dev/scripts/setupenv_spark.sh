#!/usr/bin/env bash

printf "%s\n" "${SPARK_HOME:?You must set SPARK_HOME}"
printf "%s\n" "${LIVY_HOME:?You must set LIVY_HOME}"

BOOTSTRAP_MODE=$1

source "${WSHOME}/le-dev/scripts/check_aws_creds_expiration.sh"
check_aws_creds_expiration

if [[ "${BOOTSTRAP_MODE}" = "bootstrap" ]]; then
    echo "Bootstrapping Spark ..."

    SPARK_VERSION=2.4.7
    ARTIFACT_DIR="$WSHOME/le-dev/artifacts"

    if [[ ! -f "${ARTIFACT_DIR}/spark-${SPARK_VERSION}.tgz" ]]; then
        SPARK_TGZ_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz"
        wget ${SPARK_TGZ_URL} -O ${ARTIFACT_DIR}/spark-${SPARK_VERSION}.tgz
    fi

    if [[ -d "${ARTIFACT_DIR}/spark-${SPARK_VERSION}-bin-hadoop2.7" ]]; then
        rm -rf "${ARTIFACT_DIR}/spark-${SPARK_VERSION}-bin-hadoop2.7"
    fi
    tar -xzf "${ARTIFACT_DIR}/spark-${SPARK_VERSION}.tgz" -C "${ARTIFACT_DIR}"

    if [[ -d "${SPARK_HOME}" ]]; then
        sudo rm -rf "${SPARK_HOME}"
    fi
    sudo mv "${ARTIFACT_DIR}/spark-${SPARK_VERSION}-bin-hadoop2.7" "${SPARK_HOME}"

    # match spark build dependency: $SPARK_HOME/jars
    HADOOP_AWS_VERSION="2.7.3"
    if [[ ! -f "${ARTIFACT_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" ]]; then
        MAVEN_URL="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
        wget --no-check-certificate ${MAVEN_URL} -O "${ARTIFACT_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
    fi
    sudo cp "${ARTIFACT_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" "${SPARK_HOME}/jars"

    # match hadoop-aws version
    # https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/2.7.3
    AWS_SDK_VERSION=1.7.4
    if [[ ! -f "${ARTIFACT_DIR}/aws-java-sdk-${AWS_SDK_VERSION}.jar" ]]; then
        MAVEN_URL="https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/${AWS_SDK_VERSION}/aws-java-sdk-${AWS_SDK_VERSION}.jar"
        wget --no-check-certificate ${MAVEN_URL} -O "${ARTIFACT_DIR}/aws-java-sdk-${AWS_SDK_VERSION}.jar"
    fi
    sudo cp "${ARTIFACT_DIR}/aws-java-sdk-${AWS_SDK_VERSION}.jar" "${SPARK_HOME}/jars"

    PARANAMER_PATH="${HOME}/.m2/repository/com/thoughtworks/paranamer/paranamer/2.8/paranamer-2.8.jar"
    if [[ ! -f "${PARANAMER_PATH}" ]]; then
        MAVEN_URL="https://repo1.maven.org/maven2/com/thoughtworks/paranamer/paranamer/2.8/paranamer-2.8.jar"
        wget ${MAVEN_URL} -O "${PARANAMER_PATH}"
    fi

    sudo chown -R "${USER}" "${SPARK_HOME}"
fi

cp -f "${WSHOME}/le-dev/spark/spark-defaults.conf" "${SPARK_HOME}/conf"

if [[ "${BOOTSTRAP_MODE}" = "bootstrap" ]]; then
    echo "Bootstrapping Livy ..."

    LIVY_VERSION=0.7.0-incubating

    ARTIFACT_DIR="$WSHOME/le-dev/artifacts"
    ARTIFACT_NAME=apache-livy-${LIVY_VERSION}-bin

    if [[ ! -f "${ARTIFACT_DIR}/${ARTIFACT_NAME}.zip" ]]; then
        ARTIFACT_URL="s3://latticeengines-test-artifacts/artifacts/livy/apache-livy-${LIVY_VERSION}-bin.zip"
        aws s3 cp ${ARTIFACT_URL} ${ARTIFACT_DIR}/${ARTIFACT_NAME}.zip
#        APACHE_MIRROR=$(curl -s 'https://www.apache.org/dyn/closer.cgi?as_json=1' | jq --raw-output '.preferred')
#        echo "Use apache mirror: ${APACHE_MIRROR}"
#        LIVY_TGZ_URL="${APACHE_MIRROR}/incubator/livy/${LIVY_VERSION}/${ARTIFACT_NAME}.zip"
#        wget ${LIVY_TGZ_URL} -O "${ARTIFACT_DIR}/${ARTIFACT_NAME}.zip"
    fi

    if [[ -d "${ARTIFACT_DIR}/${ARTIFACT_NAME}" ]]; then
        rm -rf "${ARTIFACT_DIR}/${ARTIFACT_NAME}"
    fi
    unzip "${ARTIFACT_DIR}/${ARTIFACT_NAME}.zip" -d "${ARTIFACT_DIR}"

    if [[ -d "${LIVY_HOME}" ]]; then
        sudo rm -rf "${LIVY_HOME}"
    fi
    sudo mv "${ARTIFACT_DIR}/${ARTIFACT_NAME}" "${LIVY_HOME}"
    sudo chown -R "${USER}" "${LIVY_HOME}"
    mkdir "${LIVY_HOME}/logs"
    sudo chmod a+w "${LIVY_HOME}/logs"
fi

cp -f "${WSHOME}/le-dev/spark/livy.conf" "${LIVY_HOME}/conf"
