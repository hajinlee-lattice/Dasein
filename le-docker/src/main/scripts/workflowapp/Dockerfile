FROM latticeengines/jre:1.8

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
RUN mkdir -p /etc/ledp

ADD workflowapp.jar /workflowapp.jar

COPY cacerts /etc/pki/java/cacerts
COPY start.sh /start.sh

COPY conf/env/devcluster/latticeengines.properties /tmp/conf/env/devcluster/latticeengines.properties
COPY conf/env/qacluster/latticeengines.properties /tmp/conf/env/qacluster/latticeengines.properties
COPY conf/env/prodcluster/latticeengines.properties /tmp/conf/env/prodcluster/latticeengines.properties

CMD ["bash", "/start.sh"]


