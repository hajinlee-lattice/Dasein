#!/usr/bin/env bash
mkdir /var/log/ledp
chmod a+w /var/log/ledp

chown -R tomcat ${CATALINA_HOME}

${CATALINA_HOME}/bin/catalina.sh run


