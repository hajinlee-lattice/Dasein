service rngd restart 
service snmpd restart 
service ntpd restart 
service sshd restart
/bin/bash /opt/apache-tomcat-8.5.8/bin/catalina.sh run
