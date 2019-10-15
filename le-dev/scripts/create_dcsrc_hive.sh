#!/bin/bash
#This script is only to create hive table for datacloud source

SOURCE=$1
VERSION=$2

if [ -z "$SOURCE" ] || [ -z "$VERSION" ]
then
	echo "Please provide source name and version"
	exit
fi

if [[ ! $VERSION =~ ^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])_[0-5][0-9]-[0-5][0-9]-[0-5][0-9]_UTC ]]
then
	echo "Please provide valid version"
	exit
fi

TABLE_SRC=$(echo "$SOURCE" | tr '[:upper:]' '[:lower:]')
TABLE_VER=$(echo "$VERSION" | tr '[:upper:]' '[:lower:]')
TABLE_VER=$(echo "$TABLE_VER" | tr - _)
TABLE="ldc_${TABLE_SRC}_${TABLE_VER}"

QUERY="CREATE EXTERNAL TABLE $TABLE ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' LOCATION '/Pods/Production/Services/PropData/Sources/$SOURCE/Snapshot/$VERSION' TBLPROPERTIES('avro.schema.url'='/Pods/Production/Services/PropData/Sources/$SOURCE/Schema/$VERSION/$SOURCE.avsc');"
beeline -u jdbc:hive2://bodcprodvhdp50.prod.lattice.local:10015 -n yarn -e "$QUERY"
echo "Created hive table $TABLE for source $SOURCE@$VERSION"
