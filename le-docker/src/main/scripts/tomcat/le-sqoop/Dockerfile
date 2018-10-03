FROM latticeengines/tomcat:9.0

RUN rm -rf $CATALINA_HOME/webapps/ROOT \
    && rm -rf $CATALINA_HOME/webapps/examples \
    && rm -rf $CATALINA_HOME/webapps/docs \
    && mkdir -p $CATALINA_HOME/webapps/ROOT \
    && echo "<!DOCTYPE html PUBLIC \"-//IETF//DTD HTML 2.0//EN\"><HTML><BODY><H1>It works!</H1></BODY></HTML>" > $CATALINA_HOME/webapps/ROOT/index.html

# put a random value here to invalidate docker cache
RUN echo invalidate cache 1538519125 > /dev/null
COPY webapps/sqoop.war $CATALINA_HOME/webapps/sqoop.war
COPY context.xml $CATALINA_HOME/webapps/manager/META-INF/context.xml
COPY context.xml $CATALINA_HOME/webapps/host-manager/META-INF/context.xml
COPY tomcat-users.xml $CATALINA_HOME/conf/tomcat-users.xml
COPY catalina.sh $CATALINA_HOME/bin/catalina.sh

COPY webapps/latticeengines.properties /etc/ledp/latticeengines.properties
COPY start.sh /start.sh
