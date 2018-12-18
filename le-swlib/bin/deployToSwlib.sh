#!/bin/bash

# Must be run from le-swlib directory
JARFILE=`ls target/le-swlib-*-shaded.jar`
VERSION=`echo $JARFILE | cut -d \- -f 3`
SNAPSHOT=`echo $JARFILE | cut -d \- -f 4`

if test $SNAPSHOT = "SNAPSHOT"
    then
        VERSION=$VERSION-"SNAPSHOT"
fi
echo "$@"

# If -v exists in the arguments then use that version. If not, then use the version of the library
if [[ $@ =~ .*\-v.* ]]
    then
        java -Dlog4j.configurationFile=bin/log4j2.xml -cp $JARFILE:$HADOOP_CONF_DIR com.latticeengines.swlib.SwlibTool "$@"
    else
        java -Dlog4j.configurationFile=bin/log4j2.xml -cp $JARFILE:$HADOOP_CONF_DIR com.latticeengines.swlib.SwlibTool "$@" -v $VERSION
fi

