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

RUN mkdir -p $CATALINA_HOME/webapps/ROOT \
    && echo "<!DOCTYPE html PUBLIC \"-//IETF//DTD HTML 2.0//EN\"><HTML><BODY><H1>It works!</H1></BODY></HTML>" > $CATALINA_HOME/webapps/ROOT/index.html

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
COPY context.xml $CATALINA_HOME/webapps/manager/META-INF/context.xml
COPY context.xml $CATALINA_HOME/webapps/host-manager/META-INF/context.xml
COPY server.xml $CATALINA_HOME/conf/server.xml
COPY tomcat-users.xml $CATALINA_HOME/conf/tomcat-users.xml

COPY cacerts /etc/pki/java/cacerts
COPY tomcat.jks /etc/pki/java/tomcat.jks
COPY log4j.properties /log4j.properties
COPY META-INF/context.xml /META-INF/context.xml
COPY conf/env/qacluster_aws/latticeengines.properties /tmp/conf/env/qacluster/latticeengines.properties
COPY conf/env/prodcluster_aws/latticeengines.properties /tmp/conf/env/prodcluster/latticeengines.properties
COPY start.sh /start.sh

EXPOSE 8080 8443 1099
CMD ["bash", "/start.sh"]