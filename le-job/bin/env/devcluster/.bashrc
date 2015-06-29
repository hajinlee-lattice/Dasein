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
export DPQUARTZ_ENABLED=disabled
export DPDL_QUARTZ_ENABLED=disabled
export PROPDATA_MADISON_QUARTZ_ENABLED=disabled
export SCORING_MGR_QUARTZ_ENABLED=disabled
export DELLEBI_QUARTZ_ENABLED=disabled
export MODEL_DL_QUARTZ_ENABLED=disabled
export DRIVEN_API_KEY=4A0FBB2222A74F6CA27A541641D62597
