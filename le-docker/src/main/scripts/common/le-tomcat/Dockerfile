FROM latticeengines/jre:1.8
RUN groupadd -r tomcat && useradd -r -g tomcat tomcat
WORKDIR /opt

RUN yum install -y openssl-devel expat-devel jq && yum groupinstall -y "Development Tools" && yum clean all

# install APR
ENV APR_VERSION=1.6.3
ENV APR_UTIL_VERSION=1.6.1
RUN curl https://archive.apache.org/dist/apr/KEYS | gpg --import \
    && gpg --keyserver pgpkeys.mit.edu --recv-key 9088F565

RUN APACHE_MIRROR=$(curl -s 'https://www.apache.org/dyn/closer.cgi?as_json=1' | jq --raw-output '.preferred') \
    && wget ${APACHE_MIRROR}apr/apr-${APR_VERSION}.tar.gz -O apr-${APR_VERSION}.tar.gz \
    && curl https://archive.apache.org/dist/apr/apr-${APR_VERSION}.tar.gz.asc > apr-${APR_VERSION}.tar.gz.asc \
    && gpg --verify apr-${APR_VERSION}.tar.gz.asc apr-${APR_VERSION}.tar.gz \
    && tar -zxvf apr-${APR_VERSION}.tar.gz && rm apr-${APR_VERSION}.tar.gz \
    && cd apr-${APR_VERSION} \
    && ./configure && make && make install \
    && cd .. && rm -rf apr-${APR_VERSION}

RUN APACHE_MIRROR=$(curl -s 'https://www.apache.org/dyn/closer.cgi?as_json=1' | jq --raw-output '.preferred') \
    && wget ${APACHE_MIRROR}apr/apr-util-${APR_UTIL_VERSION}.tar.gz -O apr-util-${APR_UTIL_VERSION}.tar.gz \
    && curl https://archive.apache.org/dist/apr/apr-util-${APR_UTIL_VERSION}.tar.gz.asc > apr-util-${APR_UTIL_VERSION}.tar.gz.asc \
    && cat apr-util-${APR_UTIL_VERSION}.tar.gz.asc \
    && gpg --verify apr-util-${APR_UTIL_VERSION}.tar.gz.asc apr-util-${APR_UTIL_VERSION}.tar.gz \
    && tar -zxvf apr-util-${APR_UTIL_VERSION}.tar.gz && rm apr-util-${APR_UTIL_VERSION}.tar.gz \
    && cd apr-util-${APR_UTIL_VERSION} \
    && ./configure --with-apr=/usr/local/apr \
    && make && make install \
    && cd .. && rm -rf apr-util-${APR_UTIL_VERSION}

ENV TOMCAT_VERSION=8.5.23
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

# install tomcat native
RUN cd $CATALINA_HOME/bin \
    && tar -zxvf tomcat-native.tar.gz \
    && cd tomcat-native-*-src/native \
    && ./configure --with-apr=/usr/local/apr --with-java-home=${JAVA_HOME} \
    && make && make install

RUN yum groupremove -y "Development Tools" && yum clean all

ENV CATALINA_HOME /opt/apache-tomcat-${TOMCAT_VERSION}
ENV CATALINA_OPTS -Djava.library.path=/usr/local/apr/lib
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


