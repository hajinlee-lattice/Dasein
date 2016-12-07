FROM latticeengines/centos7

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
COPY conf/env/devcluster_aws/latticeengines.properties /tmp/conf/env/devcluster/latticeengines.properties
COPY conf/env/qacluster_aws/latticeengines.properties /tmp/conf/env/qacluster/latticeengines.properties
COPY conf/env/prodcluster_aws/latticeengines.properties /tmp/conf/env/prodcluster/latticeengines.properties

VOLUME [ "/tmp/conf/env" ]

