#!/usr/bin/env bash

VERSION=`cat ../le-parent/pom.xml | grep \<version\> | head -n 1 | cut -d \< -f 2 | cut -d \> -f 2`

rm -rf $CATALINA_HOME/webapps/ms/*.war

for f in $(find . -name "*.war"); do
    fn=`echo $f | cut -d - -f 2`
    if [ $fn = "microservice" ]
    then
        fn=ROOT
    fi
    cp $f $CATALINA_HOME/webapps/ms/$fn\#\#$VERSION.war;
done

ls $CATALINA_HOME/webapps/ms/*.war