#!/usr/bin/env bash

BOOTSTRAP_MODE=$1
ARTIFACT_DIR=${WSHOME}/le-dev/artifacts

if [[ "${BOOTSTRAP_MODE}" = "bootstrap" ]]; then
    echo "Bootstrapping tomcat ..."
    TOMCAT_MAJOR=9
    TOMCAT_VERSION=9.0.21

    sudo rm -rf $CATALINA_HOME
    sudo mkdir -p ${CATALINA_HOME} || true
    sudo chown -R ${USER} ${CATALINA_HOME} || true

    if [[ ! -f "${ARTIFACT_DIR}/apache-tomcat-${TOMCAT_VERSION}.tar.gz" ]]; then
        APACHE_MIRROR=$(curl -s 'https://www.apache.org/dyn/closer.cgi?as_json=1' | jq --raw-output '.preferred')
        TOMCAT_TGZ_URL="${APACHE_MIRROR}tomcat/tomcat-${TOMCAT_MAJOR}/v${TOMCAT_VERSION}/bin/apache-tomcat-${TOMCAT_VERSION}.tar.gz"
        TOMCAT_TGZ_ARCHIVE_URL="https://archive.apache.org/dist/tomcat/tomcat-9/v${TOMCAT_VERSION}/bin/apache-tomcat-${TOMCAT_VERSION}.tar.gz"
        TOMCAT_TGZ="${ARTIFACT_DIR}/apache-tomcat-${TOMCAT_VERSION}.tar.gz"
	    # if active apache mirror cannot find the version, fall back to archive server
        wget "${TOMCAT_TGZ_URL}" -O ${TOMCAT_TGZ} || wget ${TOMCAT_TGZ_ARCHIVE_URL} -O ${TOMCAT_TGZ}
    fi
    rm -rf ${ARTIFACT_DIR}/apache-tomcat-${TOMCAT_VERSION} || true
    tar xzf ${ARTIFACT_DIR}/apache-tomcat-${TOMCAT_VERSION}.tar.gz -C ${ARTIFACT_DIR}
    cp -rf ${ARTIFACT_DIR}/apache-tomcat-${TOMCAT_VERSION}/* ${CATALINA_HOME}
    rm -rf ${CATALINA_HOME}/webapps/examples
    rm -rf ${CATALINA_HOME}/webapps/host-manager
    rm -rf ${CATALINA_HOME}/webapps/docs
    rm -rf ${CATALINA_HOME}/webapps/ROOT

    UNAME=`uname`
    if [[ "${UNAME}" == 'Darwin' ]]; then
        echo "You are on Mac"
        APR_VERSION=`brew list apr | head -n 1 | cut -d / -f 6`
        echo "You installed apr ${APR_VERSION}"
        OPENSSL_VERSION=`brew list openssl | head -n 1 | cut -d / -f 6`
        echo "You installed openssl ${OPENSSL_VERSION}"
        pushd ${CATALINA_HOME}/bin
        tar xzf tomcat-native.tar.gz
        cd tomcat-native-*-src/native
        ./configure \
            --with-java-home=${JAVA_HOME} \
            --with-apr=/usr/local/Cellar/apr/${APR_VERSION}/ \
            --with-ssl=/usr/local/Cellar/openssl/${OPENSSL_VERSION} \
            --prefix=${CATALINA_HOME}
        make && make install
        popd
    else
        echo "You are on ${UNAME}"
        pushd ${CATALINA_HOME}/bin
        tar xzf tomcat-native.tar.gz
        cd tomcat-native-*-src/native
        ./configure \
            --with-java-home=${JAVA_HOME} \
            --prefix=${CATALINA_HOME}
        make && make install
        popd
    fi

    sudo mkdir -p /etc/ledp/tls
    sudo chown -R ${USER} /etc/ledp/tls
    rm -rf /etc/ledp/tls/*
    aws s3 cp s3://latticeengines-test-artifacts/artifacts/tls/star.lattice.local.crt /etc/ledp/tls/server.crt
    aws s3 cp s3://latticeengines-test-artifacts/artifacts/tls/star.lattice.local.key /etc/ledp/tls/server.key
    aws s3 cp s3://latticeengines-test-artifacts/artifacts/tls/ledp_keystore.jks /etc/ledp/tls/ledp_keystore.jks
    aws s3 cp s3://latticeengines-test-artifacts/artifacts/tls/cacerts /etc/ledp/tls/cacerts
    chmod 600 /etc/ledp/tls/server.crt
    chmod 600 /etc/ledp/tls/server.key
    chmod 600 /etc/ledp/tls/ledp_keystore.jks
    chmod 600 /etc/ledp/tls/cacerts
fi

for file in 'server.xml' 'web.xml' 'context.xml' 'catalina.properties' 'tomcat-users.xml'; do
    cp -f ${CATALINA_HOME}/conf/${file} ${CATALINA_HOME}/conf/${file}.BAK
    cp -f ${WSHOME}/le-dev/tomcat/${file} ${CATALINA_HOME}/conf/${file}
done

cp ${CATALINA_HOME}/bin/catalina.sh ${CATALINA_HOME}/bin/catalina.sh.BAK
cp ${WSHOME}/le-dev/tomcat/catalina.sh ${CATALINA_HOME}/bin/catalina.sh

mkdir -p ${CATALINA_HOME}/webapps/ms || true
cp -r ${CATALINA_HOME}/webapps/manager ${CATALINA_HOME}/webapps/ms
chmod -R 775 ${CATALINA_HOME}/webapps/ms/manager

sudo mkdir -p /var/log/ledp || true
sudo chmod a+w /var/log/ledp

sudo mkdir -p /var/log/scoring/mapper || true
sudo chmod a+w /var/log/scoring/mapper

sudo mkdir -p /var/cache/scoringapi || true
sudo chmod a+w /var/cache/scoringapi
