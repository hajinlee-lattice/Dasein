FROM latticeengines/tomcat-jdk-11

RUN rm -rf $CATALINA_HOME/webapps/ROOT \
    && rm -rf $CATALINA_HOME/webapps/examples \
    && rm -rf $CATALINA_HOME/webapps/docs \
    && mkdir -p $CATALINA_HOME/webapps/ROOT \
    && echo "<!DOCTYPE html PUBLIC \"-//IETF//DTD HTML 2.0//EN\"><HTML><BODY><H1>It works!</H1></BODY></HTML>" > $CATALINA_HOME/webapps/ROOT/index.html

RUN echo "admin lattice1" > ${CATALINA_HOME}/conf/jmxremote.password && chmod 600 ${CATALINA_HOME}/conf/jmxremote.password
RUN echo "admin readwrite" > ${CATALINA_HOME}/conf/jmxremote.access

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
COPY context.xml $CATALINA_HOME/webapps/manager/META-INF/context.xml
COPY context.xml $CATALINA_HOME/webapps/host-manager/META-INF/context.xml
COPY server.xml $CATALINA_HOME/conf/server.xml
COPY tomcat-users.xml $CATALINA_HOME/conf/tomcat-users.xml

COPY cacerts /etc/pki/java/cacerts
COPY tomcat.jks /etc/pki/java/tomcat.jks
COPY META-INF/context.xml /META-INF/context.xml
COPY conf/env/devcluster/latticeengines.properties /tmp/conf/env/devcluster/latticeengines.properties
COPY conf/env/qacluster/latticeengines.properties /tmp/conf/env/qacluster/latticeengines.properties
COPY conf/env/prodcluster/latticeengines.properties /tmp/conf/env/prodcluster/latticeengines.properties
COPY conf/env/prodcluster_dr/latticeengines.properties /tmp/conf/env/prodcluster_dr/latticeengines.properties
COPY start.sh /start.sh

ENV LE_SECRET_KEY I03TMIIUftFUUI7bV0zFBw==
ENV LE_SALT_HINT bi0mpJJNxiYpEka5C6JO4g==

EXPOSE 8443 1098 1099
CMD ["bash", "/start.sh"]
