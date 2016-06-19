FROM sheepkiller/kafka-manager

# zookeeper
RUN wget http://supergsego.com/apache/zookeeper/zookeeper-3.4.8/zookeeper-3.4.8.tar.gz

ENV ZK_LOG_DIR /usr/zookeeper/logs
ENV ZOOKEEPER_HOME /usr/zookeeper

ENV PATH $PATH:$ZOOKEEPER_HOME/bin

RUN tar xzf zookeeper-3.4.8.tar.gz \
	&& mv zookeeper-3.4.8 /usr/zookeeper \
	&& mkdir -p /usr/zookeeper/logs \
	&& rm zookeeper-3.4.8.tar.gz

RUN mkdir -p /usr/zookeeper/conf && \
    touch /usr/zookeeper/conf/zoo.cfg && \
    echo "dataDir=/var/lib/zookeeper" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "clientPort=2181" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "tickTime=2000" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "initLimit=10" >> /usr/zookeeper/conf/zoo.cfg && \
    echo "syncLimit=5" >> /usr/zookeeper/conf/zoo.cfg

VOLUME ["/usr/zookeeper/conf"]

RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
ADD start.sh /start.sh

ENTRYPOINT [ "bash" ]
CMD [ "/start.sh" ]


