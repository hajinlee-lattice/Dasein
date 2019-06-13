FROM latticeengines/tomcat-jdk-8

RUN rm -rf $CATALINA_HOME/webapps/ROOT \
    && rm -rf $CATALINA_HOME/webapps/examples \
    && rm -rf $CATALINA_HOME/webapps/docs \
    && mkdir -p $CATALINA_HOME/webapps/ROOT \
    && echo "<!DOCTYPE html PUBLIC \"-//IETF//DTD HTML 2.0//EN\"><HTML><BODY><H1>It works!</H1></BODY></HTML>" > $CATALINA_HOME/webapps/ROOT/index.html

ADD hadoop-2.8.5.tar.gz /opt/

ENV HADOOP_HOME /opt/hadoop-2.8.5
ENV HADOOP_MAPRED_HOME $HADOOP_HOME
ENV HADOOP_COMMON_HOME $HADOOP_HOME
ENV HADOOP_HDFS_HOME $HADOOP_HOME
ENV HADOOP_YARN_HOME $HADOOP_HOME
ENV HADOOP_CONF_DIR ${HADOOP_HOME}/etc/hadoop
ENV HADOOP_COMMON_DIR ${HADOOP_HOME}/share/common

# put a random value here to invalidate docker cache
RUN echo invalidate cache {{TIMESTAMP}} > /dev/null

COPY webapps/sqoop.war $CATALINA_HOME/webapps/sqoop.war
COPY context.xml $CATALINA_HOME/webapps/manager/META-INF/context.xml
COPY context.xml $CATALINA_HOME/webapps/host-manager/META-INF/context.xml
COPY tomcat-users.xml $CATALINA_HOME/conf/tomcat-users.xml
COPY server.xml $CATALINA_HOME/conf/server.xml
COPY log4j2.xml $CATALINA_HOME/conf/log4j2.xml
COPY setenv.sh $CATALINA_HOME/bin/setenv.sh

COPY webapps/latticeengines.properties /etc/ledp/latticeengines.properties
COPY start.sh /start.sh

EXPOSE 8081
CMD ["bash", "/start.sh"]
