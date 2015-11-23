export HADOOP_HOME=/usr/hdp/current/hadoop-client
export HADOOP_MAPRED_HOME=/usr/hdp/current/hadoop-mapreduce-client
export HADOOP_CONF=/etc/hadoop/conf
export TEZ_CONF=/etc/tez/conf
export SQOOP_HOME=/usr/hdp/current/sqoop-server
export JAVA_HOME=/usr/java/default
export PATH=$JAVA_HOME/bin:$PATH

java -cp /etc/hadoop/conf.empty:$HADOOP_CONF:$TEZ_CONF:$JAVA_HOME/lib/tools.jar:lib/*:propdata.jar:. \
	-Dlog4j.configuration=file:`pwd`/log4j.properties \
	com.latticeengines.propdata.collection.job.ArchiveJobRunner ${@:1}