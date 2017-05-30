FROM latticeengines/jre:1.8
RUN groupadd -r tomcat && useradd -r -g tomcat tomcat
WORKDIR /opt

RUN wget -q http://10.41.1.10/tars/apache-tomcat-8.5.8.tar.gz -O /root/chefcache/apache-tomcat-8.5.8.tar.gz \
    && tar xfz  /root/chefcache/apache-tomcat-8.5.8.tar.gz \
    && rm -f /root/chefcache/apache-tomcat-8.5.8.tar.gz
RUN wget  -q  http://10.41.1.10/tars/apache-tomcat-8.5.8-deployer.tar.gz -O  /root/chefcache/apache-tomcat-8.5.8-deployer.tar.gz \
    && tar xfz /root/chefcache/apache-tomcat-8.5.8-deployer.tar.gz \
    && rm -f  /root/chefcache/apache-tomcat-8.5.8-deployer.tar.gz
RUN wget -q http://10.41.1.10/tars/catalina-jmx-remote.jar -O  apache-tomcat-8.5.8-deployer/lib/catalina-jmx-remote.jar \
    && wget -q http://10.41.1.10/tars/catalina-ws.jar -O  apache-tomcat-8.5.8-deployer/lib/catalina-ws.jar \
    && cp apache-tomcat-8.5.8-deployer/lib/catalina-jmx-remote.jar apache-tomcat-8.5.8/lib/catalina-jmx-remote.jar \
    && chown -R tomcat.tomcat /opt/apache-tomcat-8.5.8  

ENV CATALINA_HOME /opt/apache-tomcat-8.5.8
ENV JAVA_HOME /usr/java/default
EXPOSE 8080 8443

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
COPY files/startup.sh /
COPY files/server.xml $CATALINA_HOME/conf/server.xml
COPY files/tomcat.jks /etc/pki/java/tomcat.jks

CMD [ "/bin/bash", "/startup.sh" ]


