FROM latticeengines/tomcatbase

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
COPY webapps/{{WAR}}.war /{{WAR}}.war

RUN mkdir {{WAR}} \
    && cd {{WAR}} \
    && jar xvf /{{WAR}}.war \
    && cd .. \
    && rm -rf /{{WAR}}.war \
    && cp -f /log4j.properties {{WAR}}/WEB-INF/classes/log4j.properties \
    && cp -r /META-INF {{WAR}} \
    && rm -r -f ${CATALINA_HOME}/webapps/ROOT* \
    && cp -r {{WAR}} ${CATALINA_HOME}/webapps/ \

