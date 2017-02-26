#!/usr/bin/env bash

BOOTSTRAP_MODE=$1

if [ "${BOOTSTRAP_MODE}" = "bootstrap" ]; then
    echo "Bootstrapping tomcat ..."
    ARTIFACT_DIR=$WSHOME/le-dev/artifacts

    sudo rm -rf $CATALINA_HOME
    sudo mkdir -p ${CATALINA_HOME} || true
    sudo chown -R $USER ${CATALINA_HOME} || true

    if [ ! -f "${ARTIFACT_DIR}/apache-tomcat-8.5.11.tar.gz" ]; then
        wget http://apache.claz.org/tomcat/tomcat-8/v8.5.11/bin/apache-tomcat-8.5.11.tar.gz -O $ARTIFACT_DIR/apache-tomcat-8.5.11.tar.gz
    fi
    rm -rf $ARTIFACT_DIR/apache-tomcat-8.5.11 || true
    tar xzf $ARTIFACT_DIR/apache-tomcat-8.5.11.tar.gz -C $ARTIFACT_DIR
    cp -rf $ARTIFACT_DIR/apache-tomcat-8.5.11/* $CATALINA_HOME
    rm -rf $CATALINA_HOME/webapps/examples
    rm -rf $CATALINA_HOME/webapps/host-manager
    rm -rf $CATALINA_HOME/webapps/docs
    rm -rf $CATALINA_HOME/webapps/ROOT
fi

cp $WSHOME/le-dev/tomcat/server.xml $CATALINA_HOME/conf/server.xml
cp $WSHOME/le-dev/tomcat/catalina.sh $CATALINA_HOME/bin/catalina.sh

mkdir -p $CATALINA_HOME/webapps/ms || true

sudo mkdir -p /var/log/ledp || true
sudo chmod a+w /var/log/ledp

sudo mkdir -p /var/log/scoring/mapper || true
sudo chmod a+w /var/log/scoring/mapper

sudo mkdir -p /var/cache/scoringapi || true
sudo chmod a+w /var/cache/scoringapi
