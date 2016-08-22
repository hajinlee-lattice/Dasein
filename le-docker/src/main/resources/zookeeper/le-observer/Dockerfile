FROM latticeengines/zookeeper

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
ADD conf /tmp/conf
ADD start.sh /start.sh