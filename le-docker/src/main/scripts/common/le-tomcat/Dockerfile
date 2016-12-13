FROM latticeengines/centos
RUN yum install -y unzip && yum clean all
RUN groupadd -r tomcat && useradd -r -g tomcat tomcat
WORKDIR /opt
COPY binaries/* /root/chefcache/
RUN rpm -i /root/chefcache/jdk-8u101-linux-x64.rpm
RUN tar xvfz /root/chefcache/apache-tomcat-8.5.8.tar.gz \
    && tar xvfz /root/chefcache/apache-tomcat-8.5.8-deployer.tar.gz \
    && cp /root/chefcache/*.jar apache-tomcat-8.5.8-deployer/lib  \
    && chown -R tomcat.tomcat /opt/apache-tomcat-8.5.8  \
    && echo 'export JAVA_HOME=/usr/java/default' >> /etc/profile

ENV CATALINA_HOME /opt/apache-tomcat-8.5.8
ENV JAVA_HOME /usr/java/default
EXPOSE 8080 8443

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
COPY files/startup.sh /
COPY files/server.xml $CATALINA_HOME/conf/server.xml
COPY files/tomcat.jks /etc/pki/java/tomcat.jks

CMD [ "/bin/bash", "/startup.sh" ]


