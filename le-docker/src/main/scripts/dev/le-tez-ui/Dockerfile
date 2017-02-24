FROM latticeengines/tomcat

RUN rm -rf $CATALINA_HOME/webapps/*

RUN echo invalidate cache {{TIMESTAMP}} > /dev/null
ADD tez-ui.tar.gz /opt/apache-tomcat-8.5.8/webapps/tez-ui
ADD start.sh /start.sh

CMD [ "/bin/bash", "/start.sh" ]
