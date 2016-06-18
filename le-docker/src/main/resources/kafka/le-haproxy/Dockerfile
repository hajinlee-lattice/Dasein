FROM centos:7

# Install Haproxy.
RUN yum -y update; yum clean all; 
RUN yum -y install haproxy

# Add files.
ADD haproxy.cfg /etc/haproxy/haproxy.cfg
ADD start.bash /haproxy-start

# Define mountable directories.
VOLUME ["/haproxy-override"]

# Define working directory.
WORKDIR /etc/haproxy

RUN chmod a+w /etc/haproxy/haproxy.cfg

EXPOSE 80 433 9021-9023