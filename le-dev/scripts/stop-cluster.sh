# Test for required env variables
printf "%s\n" "${HADOOP_HOME:?You must set HADOOP_HOME}"
printf "%s\n" "${ZOOKEEPER_HOME:?You must set ZOOKEEPER_HOME}"

$HADOOP_HOME/sbin/hadoop-daemon.sh stop namenode
$HADOOP_HOME/sbin/hadoop-daemon.sh stop datanode
$HADOOP_HOME/sbin/yarn-daemon.sh stop resourcemanager
$HADOOP_HOME/sbin/yarn-daemon.sh stop nodemanager
$HADOOP_HOME/sbin/yarn-daemon.sh stop timelineserver
$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver
$HADOOP_HOME/sbin/kms.sh stop
$ZOOKEEPER_HOME/bin/zkServer.sh stop 

DYNAMO_PID=`jps | grep DynamoDBLocal.jar | awk '{print $1}'`
if [ -n "$DYNAMO_PID" ]; then
    echo "Killing Dynamo with pid $DYNAMO_PID"
    kill $DYNAMO_PID 
fi
