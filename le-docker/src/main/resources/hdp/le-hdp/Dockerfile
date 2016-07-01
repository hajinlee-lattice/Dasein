FROM latticeengines/ubuntu

RUN wget http://s3.amazonaws.com/public-repo-1.hortonworks.com/HDP/ubuntu14/2.x/2.4-latest/HDP-2.4-latest-ubuntu14-tars-tarball.tar.gz
RUN tar xzf HDP-2.4-latest-ubuntu14-tars-tarball.tar.gz && rm HDP-2.4-latest-ubuntu14-tars-tarball.tar.gz
RUN mv /HDP/ubuntu12/2.x/updates/2.4.2.0/tars/hadoop-2.7.1.2.4.2.0-258.tar.gz /HDP && \
    cd /HDP && tar xzf hadoop-2.7.1.2.4.2.0-258.tar.gz && \
    mv /HDP/hadoop-2.7.1.2.4.2.0-258 /opt/hadoop && \
    rm -rf /HDP

RUN wget https://s3.amazonaws.com/lattice-engines-dev/tez-0.8.2.tar.gz

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

RUN addgroup hadoop
RUN useradd -d /home/yarn -m -s /bin/bash -G hadoop yarn

RUN mkdir /var/run/sshd
RUN su yarn -c "ssh-keygen -t rsa -f ~/.ssh/id_rsa -P ''"
RUN su yarn -c "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys"

RUN echo invalidate cache {{TIMESTAMP}} > /dev/null

RUN mkdir -p /home/yarn/hdfs/namenode && \
    mkdir -p /home/yarn/hdfs/datanode && \
    mkdir $HADOOP_HOME/logs && \
    chown -R yarn:hadoop /home/yarn/hdfs/namenode && \
    chown -R yarn:hadoop /home/yarn/hdfs/datanode && \
    chown -R yarn:hadoop $HADOOP_HOME/logs && \
    chmod 1777 /tmp

ADD ssh_config /home/yarn/.ssh/config
RUN chown -R yarn:hadoop /home/yarn/.ssh
ADD conf $HADOOP_CONF_DIR
ADD upload_tez.sh /upload_tez.sh
ADD start.sh /start.sh

EXPOSE 22
EXPOSE 9000 50010 50020 50075
EXPOSE 8141 8050 8030 8025 8088 45454

VOLUME ["/home/yarn/hdfs/namenode", "/home/yarn/hdfs/datanode"]

ENTRYPOINT []
CMD ["bash", "/start.sh"]