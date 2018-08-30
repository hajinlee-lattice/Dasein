FROM latticeengines/jre:1.8.181
RUN groupadd -r tomcat && useradd -r -g tomcat tomcat
WORKDIR /opt

RUN yum install -y openssl jq && yum clean all

# install APR
ENV APR_VERSION=1.6.3
ENV APR_UTIL_VERSION=1.6.1
ENV TCNATIVE_VERSION=1.2.17
ENV TOMCAT_TAR_URL_PREFIX=http://10.41.1.10/tars

RUN wget -q ${TOMCAT_TAR_URL_PREFIX}/apr-${APR_VERSION}-1.x86_64.rpm -O /root/chefcache/apr-${APR_VERSION}-1.x86_64.rpm \
    && wget -q ${TOMCAT_TAR_URL_PREFIX}/apr-util-${APR_UTIL_VERSION}-1.x86_64.rpm -O /root/chefcache/apr-util-${APR_UTIL_VERSION}-1.x86_64.rpm \
    && wget -q ${TOMCAT_TAR_URL_PREFIX}/tcnative-${TCNATIVE_VERSION}-1.x86_64.rpm -O /root/chefcache/tcnative-${TCNATIVE_VERSION}-1.x86_64.rpm \
    && yum -y localinstall /root/chefcache/apr-${APR_VERSION}-1.x86_64.rpm /root/chefcache/apr-util-${APR_UTIL_VERSION}-1.x86_64.rpm /root/chefcache/tcnative-${TCNATIVE_VERSION}-1.x86_64.rpm



ENV TOMCAT_VERSION=9.0.11
ENV CATALINA_HOME /opt/apache-tomcat-${TOMCAT_VERSION}
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
#ENV CATALINA_OPTS -Djava.library.path=/usr/lib64
EXPOSE 8080 8443

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
COPY files/startup.sh /
COPY files/server.xml $CATALINA_HOME/conf/server.xml
COPY files/web.xml $CATALINA_HOME/conf/web.xml
COPY files/tomcat.jks /etc/pki/java/tomcat.jks
COPY files/certificate.pem /etc/pki/tls/server.crt
COPY files/key.pem /etc/pki/tls/server.key
RUN chmod 600 /etc/pki/tls/server.crt \
    && chmod 600 /etc/pki/tls/server.key \
    && /bin/rm -rf $CATALINA_HOME/webapps/docs \
    && /bin/rm -rf $CATALINA_HOME/webapps/examples \
    && /bin/rm -rf $CATALINA_HOME/webapps/host-manager \
    && /bin/rm -rf $CATALINA_HOME/webapps/manager

CMD [ "/bin/bash", "/startup.sh" ]


