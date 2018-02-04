FROM latticeengines/tomcatc7

RUN yum install -y haproxy && yum clean all

# EXPOSE 8071 8072 8073 8074 8075 8076 8080 8081 8085 1099

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null

COPY haproxy.cfg /etc/haproxy.conf

COPY start.sh /
COPY start-proxy.sh /

COPY lp.war ${CATALINA_HOME}/webapps/
COPY latticeengines.properties /etc/ledp/latticeengines.properties

EXPOSE 8080 8443 1099 2181 80
CMD ["bash", "/start.sh"]
