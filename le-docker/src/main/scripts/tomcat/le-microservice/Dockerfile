FROM latticeengines/tomcatbase

RUN rm -r -f ${CATALINA_HOME}/webapps/ROOT*
# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
COPY webapps/{{WAR}}.war ${CATALINA_HOME}/webapps/{{WAR}}.war
