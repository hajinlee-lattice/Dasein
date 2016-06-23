FROM latticeengines/kafka


# put a random value here to invalidate docker cache
RUN echo invalidate cache 12311345 > /dev/null
ADD schema-registry.properties /etc/schema-registry/schema-registry.properties
ADD start.sh /start.sh

EXPOSE 9022

CMD [ "bash", "/start.sh" ]