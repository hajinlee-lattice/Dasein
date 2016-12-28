FROM latticeengines/centos
RUN yum install -y unzip  wget && yum clean all
RUN groupadd -r tomcat && useradd -r -g tomcat tomcat
WORKDIR /opt
RUN wget -q http://10.41.1.10/tars/server-jre-8u101-linux-x64.tar.gz -O /root/chefcache/server-jre-8u101-linux-x64.tar.gz \
    && mkdir /usr/java \
    && pushd /usr/java \
    && tar xvf /root/chefcache/server-jre-8u101-linux-x64.tar.gz \
    && rm -f /root/chefcache/server-jre-8u101-linux-x64.tar.gz \
    && popd \
    && wget -q http://10.41.1.10/tars/jce_policy-8.zip -O /root/chefcache/jce_policy-8.zip  \
    && unzip /root/chefcache/jce_policy-8.zip \
    && cp -a UnlimitedJCEPolicyJDK8/*  /usr/java/jdk1.8.0_101/jre/lib/security \
    && rm -rf /root/chefcache/jce_policy-8.zip  UnlimitedJCEPolicyJDK8 \
    && rm -f /usr/java/default /usr/java/latest \
    && ln -s /usr/java/jdk1.8.0_101 /usr/java/latest \
    && ln -s /usr/java/latest /usr/java/default  \
    && chown -R root.root /usr/java
RUN update-alternatives --install /usr/bin/java java /usr/java/jdk1.8.0_101/bin/java  100 \
    && update-alternatives --set java /usr/java/jdk1.8.0_101/bin/java \
    && alternatives  --install /bin/java binjava /usr/java/latest/bin/java 1 \
    && alternatives  --install /usr/bin/java java /usr/java/latest/bin/java 1 \
    && alternatives  --install /usr/bin/javac  javac  /usr/java/latest/bin/javac 1 \
    && alternatives  --install /usr/bin/jar  jar  /usr/java/latest/bin/jar 1 \
    && alternatives  --install /usr/bin/jps  jps  /usr/java/latest/bin/jps 1 \
    && alternatives --set binjava /usr/java/latest/bin/java \
    && alternatives --set java /usr/java/latest/bin/java \
    && alternatives --set javac /usr/java/latest/bin/javac \
    && alternatives --set jar /usr/java/latest/bin/jar \
    && alternatives --set jps /usr/java/latest/bin/jps

RUN wget -q http://10.41.1.10/tars/apache-tomcat-8.5.8.tar.gz -O /root/chefcache/apache-tomcat-8.5.8.tar.gz \
    && tar xvfz  /root/chefcache/apache-tomcat-8.5.8.tar.gz \
    && rm -f /root/chefcache/apache-tomcat-8.5.8.tar.gz
RUN wget  -q  http://10.41.1.10/tars/apache-tomcat-8.5.8-deployer.tar.gz -O  /root/chefcache/apache-tomcat-8.5.8-deployer.tar.gz \
    && tar xvfz /root/chefcache/apache-tomcat-8.5.8-deployer.tar.gz \
    && rm -f  /root/chefcache/apache-tomcat-8.5.8-deployer.tar.gz
RUN  wget -q http://10.41.1.10/tars/catalina-jmx-remote.jar -O  apache-tomcat-8.5.8-deployer/lib/catalina-jmx-remote.jar \
     && wget -q http://10.41.1.10/tars/catalina-ws.jar -O  apache-tomcat-8.5.8-deployer/lib/catalina-ws.jar \
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


