FROM centos:7

# Install Haproxy.
RUN yum -y update; yum clean all; 
RUN yum -y install haproxy

# Add files.
ADD haproxy.cfg /etc/haproxy/haproxy.cfg
RUN chmod a+w /etc/haproxy/haproxy.cfg
ADD start.sh /start.sh

EXPOSE 80 433 9021-9023

CMD ["bash", "/start.sh"]