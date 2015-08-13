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
export DPQUARTZ_ENABLED=enabled
export DPDL_QUARTZ_ENABLED=enabled
export PROPDATA_MADISON_QUARTZ_ENABLED=enabled
export SCORING_MGR_QUARTZ_ENABLED=enabled
export DELLEBI_QUARTZ_ENABLED=enabled
export MODEL_DL_QUARTZ_ENABLED=enabled
export DRIVEN_API_KEY=162257B762A54896AE91369165E263D0
export DRIVEN_SERVER_HOSTS=http://bodcdevhdpweb54.lattice.local
