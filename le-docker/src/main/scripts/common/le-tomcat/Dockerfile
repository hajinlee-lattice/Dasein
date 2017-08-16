FROM latticeengines/jre:1.8
RUN groupadd -r tomcat && useradd -r -g tomcat tomcat
WORKDIR /opt

ENV TOMCAT_VERSION=8.5.15
ENV CATALINA_HOME /opt/apache-tomcat-${TOMCAT_VERSION}
ENV TOMCAT_TAR_URL_PREFIX=http://10.41.1.10/tars
RUN wget -q ${TOMCAT_TAR_URL_PREFIX}/apache-tomcat-${TOMCAT_VERSION}.tar.gz -O /root/chefcache/apache-tomcat-${TOMCAT_VERSION}.tar.gz \
    && tar xfz  /root/chefcache/apache-tomcat-${TOMCAT_VERSION}.tar.gz \
    && rm -f /root/chefcache/apache-tomcat-${TOMCAT_VERSION}.tar.gz
RUN wget  -q  ${TOMCAT_TAR_URL_PREFIX}/apache-tomcat-${TOMCAT_VERSION}-deployer.tar.gz -O  /root/chefcache/apache-tomcat-${TOMCAT_VERSION}-deployer.tar.gz \
    && tar xfz /root/chefcache/apache-tomcat-${TOMCAT_VERSION}-deployer.tar.gz \
    && rm -f  /root/chefcache/apache-tomcat-${TOMCAT_VERSION}-deployer.tar.gz
RUN wget -q ${TOMCAT_TAR_URL_PREFIX}/catalina-jmx-remote.jar -O  apache-tomcat-${TOMCAT_VERSION}/lib/catalina-jmx-remote.jar \
    && wget -q ${TOMCAT_TAR_URL_PREFIX}/catalina-ws.jar -O  apache-tomcat-${TOMCAT_VERSION}-deployer/lib/catalina-ws.jar \
    && chown -R tomcat.tomcat /opt/apache-tomcat-${TOMCAT_VERSION}

ENV CATALINA_HOME /opt/apache-tomcat-${TOMCAT_VERSION}
EXPOSE 8080 8443

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
COPY files/startup.sh /
COPY files/server.xml $CATALINA_HOME/conf/server.xml
COPY files/web.xml $CATALINA_HOME/conf/web.xml
COPY files/tomcat.jks /etc/pki/java/tomcat.jks
RUN /bin/rm -rf $CATALINA_HOME/webapps/docs \
   && /bin/rm -rf $CATALINA_HOME/webapps/examples \
   && /bin/rm -rf $CATALINA_HOME/webapps/host-manager \
   && /bin/rm -rf $CATALINA_HOME/webapps/manager

CMD [ "/bin/bash", "/startup.sh" ]


