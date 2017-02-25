FROM latticeengines/tomcat

RUN yum update && yum install -y haproxy && yum clean all

RUN rm -rf $CATALINA_HOME/webapps/ROOT \
    && rm -rf $CATALINA_HOME/webapps/examples \
    && rm -rf $CATALINA_HOME/webapps/docs \
    && cp -r $CATALINA_HOME/webapps/manager /manager \
    && mkdir -p /etc/pki/java

EXPOSE 8071 8072 8073 8074 8075 8076 8080 8081 8085 1099

# put a random value here to invalidate docker cache
RUN echo invalidate cache 1488011419 > /dev/null
COPY cacerts /etc/pki/java/cacerts
COPY context.xml $CATALINA_HOME/webapps/manager/META-INF/context.xml
COPY context.xml $CATALINA_HOME/webapps/host-manager/META-INF/context.xml
COPY context.xml /manager/META-INF/context.xml
COPY server.xml $CATALINA_HOME/conf/server.xml
COPY tomcat-users.xml $CATALINA_HOME/conf/tomcat-users.xml

COPY haproxy.cfg /etc/haproxy.conf

COPY start.sh /

CMD ["bash", "/start.sh"]
