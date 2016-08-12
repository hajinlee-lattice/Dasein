FROM latticeengines/tomcat

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
COPY webapps/ROOT.war /ROOT.war
COPY META-INF /META-INF

RUN mkdir ROOT \
    && cd ROOT \
    && jar xvf /ROOT.war \
    && cd .. \
    && rm -rf /ROOT.war \
    && cp -f /log4j.properties ROOT/WEB-INF/classes/log4j.properties \
    && cp -r /META-INF ROOT \
    && cp -r ROOT ${CATALINA_HOME}/webapps/ \

