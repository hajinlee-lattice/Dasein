#!/usr/bin/env bash

BOOTSTRAP_MODE=$1

if [ "${BOOTSTRAP_MODE}" = "bootstrap" ]; then
    echo "Bootstrapping tomcat ..."
    ARTIFACT_DIR=$WSHOME/le-dev/artifacts
    TOMCAT_MAJOR=8
    TOMCAT_VERSION=8.5.27

    sudo rm -rf $CATALINA_HOME
    sudo mkdir -p ${CATALINA_HOME} || true
    sudo chown -R $USER ${CATALINA_HOME} || true

    TOMCAT_TGZ_URL="http://archive.apache.org/dist/tomcat/tomcat-${TOMCAT_MAJOR}/v${TOMCAT_VERSION}/bin/apache-tomcat-${TOMCAT_VERSION}.tar.gz"

    if [ ! -f "${ARTIFACT_DIR}/apache-tomcat-${TOMCAT_VERSION}.tar.gz" ]; then
        wget "${TOMCAT_TGZ_URL}" -O "${ARTIFACT_DIR}/apache-tomcat-${TOMCAT_VERSION}.tar.gz"
    fi
    rm -rf ${ARTIFACT_DIR}/apache-tomcat-${TOMCAT_VERSION} || true
    tar xzf ${ARTIFACT_DIR}/apache-tomcat-${TOMCAT_VERSION}.tar.gz -C ${ARTIFACT_DIR}
    cp -rf ${ARTIFACT_DIR}/apache-tomcat-${TOMCAT_VERSION}/* ${CATALINA_HOME}
    rm -rf ${CATALINA_HOME}/webapps/examples
    rm -rf ${CATALINA_HOME}/webapps/host-manager
    rm -rf ${CATALINA_HOME}/webapps/docs
    rm -rf ${CATALINA_HOME}/webapps/ROOT

    if [ "${USE_HTTP2}" == "true" ]; then
        cd $CATALINA_HOME/bin
        tar xzf tomcat-native.tar.gz
        cd tomcat-native-1.2.16-src/native
        ./configure \
            --with-java-home=$JAVA_HOME \
            --with-apr=/usr/local/Cellar/apr/1.6.3/ \
            --with-ssl=/usr/local/Cellar/openssl/1.0.2n \
            --prefix=$CATALINA_HOME
        make && make install
    fi
fi

for file in 'server.xml' 'web.xml' 'context.xml' 'catalina.properties' 'tomcat-users.xml'; do
    cp -f ${CATALINA_HOME}/conf/${file} ${CATALINA_HOME}/conf/${file}.BAK
    cp -f ${WSHOME}/le-dev/tomcat/${file} ${CATALINA_HOME}/conf/${file}
done


sudo mkdir -p /etc/ledp/tls
sudo chown -R $USER /etc/ledp/tls
cp -f ${WSHOME}/le-dev/tomcat/server.crt /etc/ledp/tls/server.crt
cp -f ${WSHOME}/le-dev/tomcat/server.key /etc/ledp/tls/server.key
chmod 600 /etc/ledp/tls/server.crt
chmod 600 /etc/ledp/tls/server.key

if [ "${USE_HTTP2}" == "true" ]; then
    cp -f ${WSHOME}/le-dev/tomcat/server-http2.xml ${CATALINA_HOME}/conf/server.xml
fi

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
