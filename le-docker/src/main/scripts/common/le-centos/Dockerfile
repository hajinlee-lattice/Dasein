# leave the comments for now until we are sure the services will not be put back in
FROM centos:centos6.8
RUN yum update -y && yum clean all
RUN yum install -y epel-release initscripts && yum clean all
#RUN yum install -y git passwd wget which && yum clean all
#RUN yum install -y git passwd wget bind-utils net-tools which && yum clean all
RUN mkdir -p /root/.ssh  && chmod 0700  /root/.ssh   && mkdir -p /etc/snmp  && mkdir -p /opt/le/bin && chmod 755 /opt/le/bin  && mkdir /root/chefcache
COPY files/le.repo /etc/yum.repos.d/le.repo

COPY files/motd.txt /etc/motd
COPY files/selinux_config /etc/selinux/config
COPY files/certnew.cer /etc/pki/ca-trust/source/anchors/certnew.cer
RUN update-ca-trust force-enable && update-ca-trust extract


