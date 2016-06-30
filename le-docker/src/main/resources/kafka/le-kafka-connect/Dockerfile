FROM latticeengines/kafka

#RUN wget http://63.245.196.124/software/Apache/hadoop/common/hadoop-2.7.1/hadoop-2.7.1.tar.gz && \
#    tar xzf hadoop-2.7.1.tar.gz

ENV HADOOP_HOME=/hadoop-2.7.1
ENV HADOOP_CONF_DIR=/hadoop-2.7.1/etc/hadoop
ENV PATH=$PATH:/hadoop-2.7.1/bin

RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
ADD connect-distributed.properties /etc/kafka/connect-distributed.properties
ADD start.sh /start.sh

EXPOSE
EXPOSE 9024

CMD ["bash", "/start.sh"]