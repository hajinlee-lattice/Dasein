FROM latticeengines/kafka

ENV HADOOP_CONF_DIR=/etc/hadoop

RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
ADD connect-distributed.properties /etc/kafka/connect-distributed.properties
ADD start.sh /start.sh

EXPOSE
EXPOSE 9024

CMD ["bash", "/start.sh"]