# leave the comments for now until we are sure the services will not be put back in
FROM centos:centos6.8
RUN yum update -y && yum clean all
RUN yum install -y epel-release initscripts && yum clean all
RUN yum install -y postfix && yum clean all && mkfifo /var/spool/postfix/public/pickup ps aux && chkconfig postfix on
#RUN yum install -y git passwd wget which && yum clean all
#RUN yum install -y git passwd wget bind-utils net-tools which && yum clean all
RUN mkdir -p /root/.ssh  && chmod 0700  /root/.ssh   && mkdir -p /etc/snmp  && mkdir -p /opt/le/bin && chmod 755 /opt/le/bin  && mkdir /root/chefcache && mkdir /root/postfix
#RUN mkfifo /var/spool/postfix/public/pickup ps aux
COPY files/le.repo /etc/yum.repos.d/le.repo

COPY files/main.cf.dev files/main.cf.production /root/postfix/
COPY files/main.cf.dev /etc/postfix/main.cf
COPY files/motd.txt /etc/motd
COPY files/selinux_config /etc/selinux/config
COPY files/certnew.cer /etc/pki/ca-trust/source/anchors/certnew.cer
RUN update-ca-trust force-enable && update-ca-trust extract


