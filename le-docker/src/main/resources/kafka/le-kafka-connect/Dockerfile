FROM latticeengines/kafka

ENV HADOOP_CONF_DIR=/etc/hadoop

RUN echo invalidate cache 1468970771 > /dev/null
ADD plugin /usr/share/java/kafka-connect-ledp
ADD connect-distributed.properties /etc/kafka/connect-distributed.properties
ADD start.sh /start.sh

EXPOSE 9024

CMD ["bash", "/start.sh"]