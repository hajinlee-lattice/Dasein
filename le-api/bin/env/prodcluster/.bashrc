# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
        . /etc/bashrc
fi

# User specific aliases and functions
export JETTY_HOME=/opt/jetty
export JETTY_USER=yarn
export JETTY_HOST=0.0.0.0
export JETTY_LOGS=logs
export JETTY_ARGS=jetty.host=0.0.0.0
export HADOOP_HOME=/usr/hdp/current/hadoop-client
export HADOOP_MAPRED_HOME=/usr/hdp/current/hadoop-mapreduce-client
export HADOOP_CONF=/etc/hadoop/conf
export SQOOP_HOME=/usr/hdp/current/sqoop-server
export JAVA_HOME=/usr/java/default
