In order to get this to run:

mvn clean package
mvn scala:run -DmainClass=com.latticeengines.sparkdb.ABCount

The following env variables need to be defined:

HADOOP_CONF_DIR - this is the directory of the hadoop cluster ($HADOOP_HOME/etc/hadoop if using hortonworks, which you should)
SPARK_YARN_APP_JAR - this is the path to the assembly jar that supports spark running on yarn
SPARK_HOME - home directory of your spark installation
SPARK_JAR - path to the jar file created by this project after doing mvn clean package