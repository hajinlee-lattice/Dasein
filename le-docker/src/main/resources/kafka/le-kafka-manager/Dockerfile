FROM sheepkiller/kafka-manager

RUN yum -y update; yum clean all;
RUN yum -y install wget;

# java
RUN wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" \
	"http://download.oracle.com/otn-pub/java/jdk/8u60-b27/jdk-8u60-linux-x64.rpm"; \
	yum -y localinstall jdk-8u60-linux-x64.rpm; \
	rm jdk-8u60-linux-x64.rpm

# zookeeper
RUN wget http://supergsego.com/apache/zookeeper/zookeeper-3.4.8/zookeeper-3.4.8.tar.gz

ENV ZK_LOG_DIR /usr/zookeeper/logs
ENV ZOOKEEPER_HOME /usr/zookeeper

ENV PATH $PATH:$ZOOKEEPER_HOME/bin

RUN tar xzf zookeeper-3.4.8.tar.gz \
	&& mkdir -p /usr \
	&& mv zookeeper-3.4.8.tar.gz /usr/zookeeper \
	&& mkdir -p /usr/zookeeper/logs \
	&& rm zookeeper-3.4.8.tar.gz

COPY zoo.cfg /usr/zookeeper/etc/zookeeper/zoo.cfg

EXPOSE 9000

