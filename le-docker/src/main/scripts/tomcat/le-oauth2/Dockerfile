FROM latticeengines/tomcatbase

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
COPY webapps/ROOT.war /ROOT.war
COPY application.properties /application.properties
COPY log4j2.xml /log4j2.xml
COPY tomcat_filters.xml /tomcat_filters.xml

RUN mkdir ROOT \
    && cd ROOT \
    && jar xvf /ROOT.war \
    && cd .. \
    && rm -rf /ROOT.war \
    && cp -f /log4j2.xml ROOT/WEB-INF/classes/log4j2.xml \
    && cp -r /META-INF ROOT \
    && rm -r -f ${CATALINA_HOME}/webapps/ROOT* \
    && cp -r ROOT ${CATALINA_HOME}/webapps/ROOT \
    && cp /application.properties ROOT/WEB-INF/classes/application.properties
