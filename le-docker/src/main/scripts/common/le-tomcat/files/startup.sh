#service rngd restart
#service snmpd restart
#service ntpd restart
#service sshd restart

export JAVA_HOME="/usr/java/jdk1.8.0_101"
/bin/bash /opt/apache-tomcat-8.5.8/bin/catalina.sh run
