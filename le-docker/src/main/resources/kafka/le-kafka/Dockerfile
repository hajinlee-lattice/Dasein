FROM centos:7

RUN yum -y update; yum clean all; 
RUN yum -y install openssh-server passwd wget;

# java
RUN wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" \
	"http://download.oracle.com/otn-pub/java/jdk/8u60-b27/jdk-8u60-linux-x64.rpm"; \
	yum -y localinstall jdk-8u60-linux-x64.rpm; \
	rm jdk-8u60-linux-x64.rpm

# confluent
ADD confluent.repo /etc/yum.repos.d/confluent.repo
RUN yum clean all; yum -y -v install confluent-platform-2.11
RUN mkdir -p /var/lib/zookeeper; touch /var/lib/zookeeper/myid

VOLUME ["/etc/kafka"]

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
ADD kafka-server-stop /usr/bin/kafka-server-stop
ADD server.properties /etc/kafka/server.properties
ADD start.sh /start.sh

EXPOSE 9092 9199

CMD ["bash", "/start.sh"]