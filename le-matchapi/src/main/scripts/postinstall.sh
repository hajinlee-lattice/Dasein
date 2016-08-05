#!/usr/bin/env bash

source /etc/profile
echo "getting environment information"
echo $LE_ENVIRONMENT

echo "repackaging wars"
cd /tmp/le-matchapi-rpm/war

mkdir ROOT
pushd ROOT
jar xvf ../ROOT.war
popd
rm -rf ROOT.war

if [ -f "/tmp/le-matchapi-rpm/env/${LE_ENVIRONMENT}/log4j.properties" ]
then
    echo "replacing log4j.properties for ROOT.war"
    cp -f /tmp/le-matchapi-rpm/env/${LE_ENVIRONMENT}/log4j.properties ROOT/WEB-INF/classes/log4j.properties
fi

jar cvf ROOT.war -C ROOT/ .

rm -rf /usr/share/tomcat/work/MatchAPIEngine
mkdir -p /usr/share/tomcat/webapps/matchapi
cp -f ROOT.war /usr/share/tomcat/webapps/matchapi

chown -R tomcat:tomcat /usr/share/tomcat/webapps/matchapi
chmod -R 775 /usr/share/tomcat/webapps/matchapi

rm -rf /tmp/le-matchapi-rpm

echo "finishing"
ll /usr/share/tomcat/webapps/matchapi/*.war