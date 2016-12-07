FROM centos:centos6.8
RUN yum update -y
RUN yum install -y epel-release
RUN yum install -y git atop net-snmp nmap iftop sysstat perf tcpdump net-snmp  ntp rng-tools  sshd openssh-server passwd wget bind-utils net-tools which
RUN mkdir -p /root/.ssh  && chmod 0700  /root/.ssh   && mkdir -p /etc/snmp  && mkdir -p /opt/le/bin && chmod 755 /opt/le/bin  && mkdir /root/chefcache
COPY files/le.repo /etc/yum.repos.d/le.repo
COPY files/sshd_config  /etc/ssh/sshd_config
COPY files/snmpd.conf /etc/snmp/snmpd.conf

COPY files/motd.txt /etc/motd
COPY files/selinux_config /etc/selinux/config
COPY files/certnew.cer /etc/pki/ca-trust/source/anchors/certnew.cer
RUN update-ca-trust force-enable && update-ca-trust extract
COPY  files/rngd.conf /etc/sysconfig/rngd
COPY  files/aws-dev.pub /root/.ssh/authorized_keys
RUN chmod 0600 /root/.ssh/authorized_keys
EXPOSE 22
COPY files/startup.sh /
CMD ["bin/bash", "/startup.sh"]


