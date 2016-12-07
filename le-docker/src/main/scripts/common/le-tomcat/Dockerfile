FROM lattice-centos
RUN yum install -y unzip  
RUN groupadd -r tomcat && useradd -r -g tomcat tomcat
WORKDIR /opt
COPY binaries/* /root/chefcache/
RUN rpm -i /root/chefcache/jdk-8u101-linux-x64.rpm
RUN tar xvfz /root/chefcache/apache-tomcat-8.5.8.tar.gz  && tar xvfz /root/chefcache/apache-tomcat-8.5.8-deployer.tar.gz && cp /root/chefcache/*.jar apache-tomcat-8.5.8-deployer/lib  && chown -R tomcat.tomcat /opt/apache-tomcat-8.5.8  && echo 'export JAVA_HOME=/usr/java/default' >> /etc/profile
EXPOSE 8080
COPY files/startup.sh /
ENTRYPOINT [ "/bin/bash", "/startup.sh" ]


