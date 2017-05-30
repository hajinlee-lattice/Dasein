FROM centos:7

RUN yum -y update; yum clean all; 
RUN yum -y install openssh-server passwd wget bind-utils net-tools which;

# java
RUN wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" \
	"http://download.oracle.com/otn-pub/java/jdk/8u60-b27/jdk-8u60-linux-x64.rpm"; \
	yum -y localinstall jdk-8u60-linux-x64.rpm; \
	rm jdk-8u60-linux-x64.rpm; \
    echo 'export JAVA_HOME=/usr/java/default' >> /etc/profile
