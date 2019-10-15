#!/bin/bash

wget http://localhost:8080/doc/status > /tmp
mv "$CATALINA_HOME/webapps/ms/quartz.war" /tmp
sleep 5
mv /tmp/quartz.war "$CATALINA_HOME/webapps/ms/quartz.war"

