FROM haproxy

RUN apt-get update
RUN apt-get -y install dnsutils

RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
COPY haproxy.cfg /usr/local/etc/haproxy/haproxy.cfg
COPY start.sh /start.sh

ENTRYPOINT ["bash"]
CMD ["/start.sh"]