FROM latticeengines/centos7

ENV HDP_VERSION 2.7.1.2.4.3.0-227

ADD hadoop-$HDP_VERSION.tar.gz /opt
RUN mv /opt/hadoop-$HDP_VERSION /opt/hadoop

ENV HADOOP_HOME /opt/hadoop
ENV HADOOP_MAPRED_HOME $HADOOP_HOME
ENV HADOOP_COMMON_HOME $HADOOP_HOME
ENV HADOOP_HDFS_HOME $HADOOP_HOME
ENV HADOOP_YARN_HOME $HADOOP_HOME
ENV HADOOP_TEZ_HOME $HADOOP_HOME
ENV YARN_HOME $HADOOP_HOME
ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop
ENV YARN_CONF_DIR HADOOP_CONF_DIR
ENV PATH $PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV CATALINA_HOME $HADOOP_HOME/share/hadoop/kms/tomcat

RUN addgroup hadoop \
    && useradd -d /home/yarn -m -s /bin/bash -G hadoop yarn \
    && chown -R yarn:hadoop /opt/hadoop \
    && mkdir /var/run/sshd \

RUN mkdir -p /home/yarn/hdfs/namenode && \
    mkdir -p /home/yarn/hdfs/datanode && \
    mkdir $HADOOP_HOME/logs && \
    chown -R yarn:hadoop /home/yarn/hdfs/namenode && \
    chown -R yarn:hadoop /home/yarn/hdfs/datanode && \
    chown -R yarn:hadoop $HADOOP_HOME/logs && \
    chmod 1777 /tmp

RUN echo invalidate cache {{TIMESTAMP}} > /dev/null

# these ssh keys are only for local env
ADD id_rsa /home/yarn/.ssh/id_rsa
ADD id_rsa.pub /home/yarn/.ssh/id_rsa.pub
ADD ssh_config /home/yarn/.ssh/config
RUN chown -R yarn:hadoop /home/yarn/.ssh \
    && chmod 600 /home/yarn/.ssh/id_rsa \
    && su yarn -c "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys"

ADD conf $HADOOP_CONF_DIR
ADD upload_tez.sh /upload_tez.sh
ADD start.sh /start.sh

EXPOSE 9000 50070 8088 8188 10200

ENTRYPOINT []
CMD ["bash", "/start.sh"]