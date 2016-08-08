FROM tomcat:8.5

RUN rm -rf $CATALINA_HOME/webapps/ROOT \
    && rm -rf $CATALINA_HOME/webapps/examples \
    && rm -rf $CATALINA_HOME/webapps/docs

RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections \
    && echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | tee /etc/apt/sources.list.d/webupd8team-java.list \
    && echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | tee -a /etc/apt/sources.list.d/webupd8team-java.list \
    && apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys EEA14886 \
    && apt-get update \
    && apt-get install -y oracle-java8-installer \
    && rm -rf /etc/apt/sources.list.d/webupd8team-java.list

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMSTAMP}} > /dev/null
COPY context.xml $CATALINA_HOME/webapps/manager/META-INF/context.xml
COPY context.xml $CATALINA_HOME/webapps/host-manager/META-INF/context.xml
COPY server.xml $CATALINA_HOME/conf/server.xml
COPY tomcat-users.xml $CATALINA_HOME/conf/tomcat-users.xml

COPY cacerts /etc/pki/java/cacerts
COPY server.key /etc/pki/java/server.key
COPY server.crt /etc/pki/java/server.crt
COPY log4j.properties /log4j.properties
COPY conf/env/qacluster/latticeengines.properties /etc/ledp/qacluster/latticeengines.properties
COPY conf/env/prodcluster/latticeengines.properties /etc/ledp/prodcluster/latticeengines.properties
COPY start.sh /start.sh

EXPOSE 8443 1099
CMD ["bash", "/start.sh"]