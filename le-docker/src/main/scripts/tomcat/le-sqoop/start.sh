#!/usr/bin/env bash
mkdir /var/log/ledp
chmod a+w /var/log/ledp

chown -R tomcat ${CATALINA_HOME}

ulimit -n 4096

${CATALINA_HOME}/bin/catalina.sh run


