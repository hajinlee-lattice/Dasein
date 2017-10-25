FROM haproxy:latest

RUN apt-get update
RUN apt-get -y install dnsutils

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
COPY haproxy.cfg /usr/local/etc/haproxy/haproxy.cfg
COPY server.pem /etc/ssl/private/server.pem
RUN chmod 600 /etc/ssl/private/server.pem
COPY start.sh /start.sh

ENTRYPOINT ["bash"]
EXPOSE 82 83
CMD ["/start.sh"]