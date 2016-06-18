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

COPY zoo.cfg /usr/zookeeper/conf/zoo.cfg
ADD start.sh /start.sh

ENTRYPOINT [ "bash" ]
CMD [ "/start.sh" ]


