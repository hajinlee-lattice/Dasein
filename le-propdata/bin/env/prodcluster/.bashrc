# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
        . /etc/bashrc
fi

ulimit -n 4096

# User specific aliases and functions
export JETTY_HOME=/opt/jetty
export JETTY_USER=yarn
export JETTY_HOST=0.0.0.0
export JETTY_LOGS=logs
export JETTY_ARGS=jetty.host=0.0.0.0
HADOOP_HOME=/usr/hdp/current/hadoop-client
HADOOP_MAPRED_HOME=/usr/current/hadoop-mapreduce-client
HADOOP_CONF=--lib=/etc/hadoop/conf
SQOOP_HOME=/usr/hdp/current/sqoop-server
export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.75.x86_64/jre
export MODEL_DL_QUARTZ_ENABLED=enabled
