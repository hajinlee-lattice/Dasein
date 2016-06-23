FROM latticeengines/kafka

VOLUME [ "/usr/share/java/kafka-connect-ledp" ]

RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
ADD connect-distributed.properties /etc/kafka/connect-distributed.properties
ADD start.sh /start.sh

EXPOSE 9024

CMD ["bash", "/start.sh"]