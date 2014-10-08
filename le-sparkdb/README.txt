In order to get this to run:

mvn clean package
scala -classpath target/le-sparkdb-1.0.3-SNAPSHOT.jar:$SPARK_YARN_APP_JAR:$HADOOP_HOME/etc/hadoop com.latticeengines.sparkdb.exposed.service.impl.SparkLauncherServiceImpl

The following env variables need to be defined if running from Eclipse:

HADOOP_CONF_DIR - this is the directory of the hadoop cluster ($HADOOP_HOME/etc/hadoop if using hortonworks, which you should)
SPARK_YARN_APP_JAR - this is the path to the assembly jar that supports spark running on yarn
SPARK_HOME - home directory of your spark installation
SPARK_JAR - path to the jar file created by this project after doing mvn clean package

You can also execute this from the command line:

$SPARK_HOME/bin/spark-submit --class com.latticeengines.sparkdb.ABCount \
    --master yarn-cluster \
    --num-executors 3  \
    --driver-memory 4g \
    --executor-memory 2g  \
    --queue Priority0 \
    --executor-cores 1 \
    target/le-sparkdb-1.0.3-SNAPSHOT.jar

