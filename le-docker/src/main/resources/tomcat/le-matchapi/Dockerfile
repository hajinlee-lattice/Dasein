FROM latticeengines/tomcat

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMSTAMP}} > /dev/null
COPY webapps/ROOT.war /ROOT.war

RUN mkdir ROOT \
    && cd ROOT \
    && jar xvf /ROOT.war \
    && cd .. \
    && rm -rf /ROOT.war \
    && cp -f /log4j.properties ROOT/WEB-INF/classes/log4j.properties \
    && jar cvf ROOT.war -C ROOT/ . \
    && cp ROOT.war ${CATALINA_HOME}/webapps/ROOT.war \
    && rm -rf ROOT.war

