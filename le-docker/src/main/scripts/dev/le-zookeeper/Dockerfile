FROM latticeengines/centos7

ENV ZK_LOG_DIR /usr/zookeeper/logs
ENV ZOOKEEPER_HOME /usr/zookeeper
ENV PATH $PATH:$ZOOKEEPER_HOME/bin

# zookeeper
RUN wget http://supergsego.com/apache/zookeeper/zookeeper-3.4.8/zookeeper-3.4.8.tar.gz \
    && tar xzf zookeeper-3.4.8.tar.gz \
	&& mv zookeeper-3.4.8 /usr/zookeeper \
	&& mkdir -p /usr/zookeeper/logs \
	&& rm zookeeper-3.4.8.tar.gz

RUN mkdir -p /usr/zookeeper/data; touch /usr/zookeeper/data/myid

RUN touch /usr/zookeeper/conf/zoo.cfg && \
    echo "dataDir=/usr/zookeeper/data" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "dataLogDir=/usr/zookeeper/data-log" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "clientPort=2181" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "tickTime=2000" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "initLimit=10" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "syncLimit=5" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "autopurge.snapRetainCount=5" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "autopurge.purgeInterval=2" >> /usr/zookeeper/conf/zoo.cfg

VOLUME ["/usr/zookeeper/data"]

RUN echo invalidate cache 1508369778 > /dev/null

ADD start.sh /start.sh

EXPOSE 2181 2888 3888

CMD ["bash", "/start.sh"]
