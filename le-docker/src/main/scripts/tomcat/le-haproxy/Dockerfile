FROM latticeengines/centos7
RUN yum install -y haproxy dnsutils && yum clean all

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
COPY haproxy.cfg /etc/haproxy/haproxy.cfg
COPY server.pem /etc/ssl/private/server.pem
RUN chmod 600 /etc/ssl/private/server.pem
COPY start.sh /start.sh

# ENTRYPOINT ["bash"]
EXPOSE 82 83
CMD ["/usr/bin/bash", "/start.sh"]
