FROM latticeengines/jre

ENV ZK_LOG_DIR /usr/zookeeper/logs
ENV ZOOKEEPER_HOME /usr/zookeeper
ENV PATH $PATH:$ZOOKEEPER_HOME/bin
ENV ZOOKEEPER_VERSION=3.4.13

# zookeeper
RUN yum install -y wget jq && yum clean all
RUN APACHE_MIRROR=$(curl -s 'https://www.apache.org/dyn/closer.cgi?as_json=1' | jq --raw-output '.preferred') \
    && wget ${APACHE_MIRROR}zookeeper/zookeeper-${ZOOKEEPER_VERSION}/zookeeper-${ZOOKEEPER_VERSION}.tar.gz \
    && tar xzf zookeeper-${ZOOKEEPER_VERSION}.tar.gz \
	&& mv zookeeper-${ZOOKEEPER_VERSION} /usr/zookeeper \
	&& mkdir -p /usr/zookeeper/logs \
	&& rm zookeeper-${ZOOKEEPER_VERSION}.tar.gz

RUN mkdir -p /usr/zookeeper/data; touch /usr/zookeeper/data/myid

RUN touch /usr/zookeeper/conf/zoo.cfg && \
    echo "dataDir=/usr/zookeeper/data" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "dataLogDir=/usr/zookeeper/data-log" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "clientPort=2181" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "tickTime=2000" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "initLimit=10" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "syncLimit=5" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "maxClientCnxns=600" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "autopurge.snapRetainCount=5" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "autopurge.purgeInterval=2" >> /usr/zookeeper/conf/zoo.cfg

VOLUME ["/usr/zookeeper/data"]

RUN echo invalidate cache {{TIMESTAMP}} > /dev/null

ADD start.sh /start.sh

EXPOSE 2181 2888 3888

CMD ["bash", "/start.sh"]
