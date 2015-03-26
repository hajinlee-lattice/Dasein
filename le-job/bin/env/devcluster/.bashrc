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
export HADOOP_HOME=/usr/lib/hadoop
export HADOOP_MAPRED_HOME=/usr/lib/hadoop
export HADOOP_CONF=/etc/hadoop/conf.empty
export SQOOP_HOME=/usr/lib/sqoop
export JAVA_HOME=/usr/java/default
export DPQUARTZ_ENABLED=disabled
export DPDL_QUARTZ_ENABLED=disabled
export PROPDATA_MADISON_QUARTZ_ENABLED=enabled
