FROM haproxy

RUN apt-get update
RUN apt-get -y install dnsutils

RUN echo invalidate cache 1466443937 > /dev/null
COPY haproxy.cfg /usr/local/etc/haproxy/haproxy.cfg
COPY start.sh /start.sh

ENTRYPOINT ["bash"]
CMD ["/start.sh"]