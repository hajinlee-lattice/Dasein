FROM centos/redis-32-centos7
USER root
RUN yum update -y && yum clean all

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
USER redis
