FROM centos:7

RUN yum -y update; yum clean all;
RUN yum -y install openssh-server passwd wget;

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
	&& mv zookeeper-3.4.8 /usr/zookeeper \
	&& mkdir -p /usr/zookeeper/logs \
	&& rm zookeeper-3.4.8.tar.gz

RUN mkdir -p /var/lib/zookeeper; touch /var/lib/zookeeper/myid

COPY zoo.cfg /usr/zookeeper/conf/zoo.cfg
ADD start.sh /start.sh

EXPOSE 2181 2888 3888

CMD ["bash", "/start.sh"]