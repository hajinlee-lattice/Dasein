FROM latticeengines/tomcat

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
COPY webapps/ROOT.war /ROOT.war

RUN mkdir ROOT \
    && cd ROOT \
    && jar xvf /ROOT.war \
    && cd .. \
    && rm -rf /ROOT.war \
    && cp -f /log4j.properties ROOT/WEB-INF/classes/log4j.properties \
    && cp -r /META-INF ROOT \
    && rm -r -f ${CATALINA_HOME}/webapps/ROOT* \
    && cp -r ROOT ${CATALINA_HOME}/webapps/ROOT \
    && cp -r ROOT ${CATALINA_HOME}/webapps/api

