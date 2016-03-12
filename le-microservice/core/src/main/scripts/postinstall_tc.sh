#!/bin/bash -x

rm -rf $CATALINA_HOME/webapps/*.war

cd /tmp/webapps

JARFILE=`ls le-microservice-*.war`
VERSION=`echo $JARFILE | cut -d \- -f 3`
SNAPSHOT=`echo $JARFILE | cut -d \- -f 4`

if test $SNAPSHOT = "SNAPSHOT"
    then
        VERSION=$VERSION-"SNAPSHOT"
fi

for f in $(find . -name "*.war"); do
    fn=`echo $f | cut -d - -f 2`
    if [ $fn = "microservice" ]
    then
        fn=doc
    fi
    cp $f $CATALINA_HOME/webapps/$fn\#\#$VERSION.war;
done

ls $CATALINA_HOME/webapps/*.war

service tomcat restart
