FROM latticeengines/centos7

ENV ZK_LOG_DIR /usr/zookeeper/logs
ENV ZOOKEEPER_HOME /usr/zookeeper
ENV PATH $PATH:$ZOOKEEPER_HOME/bin

ENV ZK_VERSION="3.4.8"
ENV ZK_TGZ_URL="https://www.apache.org/dyn/closer.cgi?action=download&filename=zookeeper/zookeeper-${ZK_VERSION}/zookeeper-${ZK_VERSION}.tar.gz"

# zookeeper
RUN wget ZK_TGZ_URL -O zookeeper-${ZK_VERSION}.tar.gz \
    && tar xzf zookeeper-${ZK_VERSION}.tar.gz \
	&& mv zookeeper-${ZK_VERSION} /usr/zookeeper \
	&& mkdir -p /usr/zookeeper/logs \
	&& rm zookeeper-${ZK_VERSION}.tar.gz

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

RUN echo invalidate cache {{TIMESTAMP}} > /dev/null

ADD start.sh /start.sh

EXPOSE 2181 2888 3888

CMD ["bash", "/start.sh"]