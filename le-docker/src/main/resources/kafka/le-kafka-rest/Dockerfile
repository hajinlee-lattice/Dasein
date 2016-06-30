FROM latticeengines/kafka


RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
ADD kafka-rest.properties /etc/kafka-rest/kafka-rest.properties
ADD start.sh /start.sh

EXPOSE
EXPOSE 9023

CMD ["bash", "/start.sh"]