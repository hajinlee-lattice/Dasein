FROM latticeengines/kafka

ENV HADOOP_CONF_DIR=/etc/hadoop

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
ADD plugin /usr/share/java/kafka-connect-ledp
ADD connect-distributed.properties /etc/kafka/connect-distributed.properties
ADD start.sh /start.sh

EXPOSE 9024

CMD ["bash", "/start.sh"]