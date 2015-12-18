java -cp $HADOOP_CONF_DIR:lib/*:propdata.jar:. \
	-Dlog4j.configuration=file:`pwd`/log4j.properties \
	com.latticeengines.propdata.collection.job.PropDataAdminTool ${@:1}