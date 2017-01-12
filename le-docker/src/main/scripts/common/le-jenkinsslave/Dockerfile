FROM latticeengines/jdk
WORKDIR /
COPY files/jenkins-slave /usr/local/bin/jenkins-slave
RUN chmod 755 /usr/local/bin/jenkins-slave
COPY files/le_yum_repo   /etc/yum.repos.d/le.repo

# collabnet
RUN wget -q http://10.41.1.10/tars/CollabNetSubversion-client-1.7.19-1.x86_64.rpm \
&& rpm -i CollabNetSubversion-client-1.7.19-1.x86_64.rpm \
&& rm -f CollabNetSubversion-client-1.7.19-1.x86_64.rpm 

# jenkins slave python
RUN yum install -y python27 \
&& yum clean all \
&& wget -q https://bootstrap.pypa.io/get-pip.py \
&& /usr/local/bin/python2.7 get-pip.py \
&& rm -rf get-pip.py

RUN yum install -y pytest-2.8.3-1.el6  pytest-pythonpath-0.7-1.el6 python-numpy-1.8.1-1.el6  scipy-0.13.3-8.el6 scikit-learn-0.14.1-1.el6 fastavro-0.7.7-1.el6  openpyxl-1.8.6-1.el6 pandas-0.13.1-1.el6 patsy-0.3.0-1.el6 statsmodels-0.5.0-1.el6 lxml-3.4.0-1.el6 avro-1.7.6-1.el6 \
&& yum clean all

RUN wget https://bootstrap.pypa.io/ez_setup.py  \
&& /usr/local/bin/python2.7 ez_setup.py \
&& rm -f ez_setup.py  setuptools-32.3.1.zip

RUN /usr/local/bin/pip2.7 --no-cache-dir install coverage \
&& /usr/local/bin/pip2.7 --no-cache-dir install py\
&& /usr/local/bin/pip2.7 --no-cache-dir install python-dateutil \
&& /usr/local/bin/pip2.7 --no-cache-dir install boto3 \
&& /usr/local/bin/pip2.7 --no-cache-dir install awscli 

RUN yum install -y python-pip python-crypto2.6 atlas  \
&& /usr/local/bin/pip2.7 --no-cache-dir install atlas \
&& /usr/bin/pip --no-cache-dir install argparse \
&& yum clean all

# jenkins slaves pacakges

RUN yum install -y rubygems  rpm-devel rpmdevtools \
&& yum clean all
WORKDIR /opt
RUN wget -q http://10.41.1.10/tars/apache-maven-3.3.3-bin.tar.gz \
&& tar xf apache-maven-3.3.3-bin.tar.gz \
&& ln -s /opt/apache-maven-3.3.3  /opt/maven \
&& rm -f apache-maven-3.3.3-bin.tar.gz

RUN yum install -y npm nodejs-grunt nodejs-grunt-cli \
&& gem install sass \
&& yum clean all

RUN /usr/local/bin/pip2.7 --no-cache-dir install kazoo 

RUN mkdir -p /var/log/ledp /var/log/scoringapi /var/cache/scoringapi \
&& chmod 0777  /var/log/ledp /var/log/scoringapi /var/cache/scoringapi 

WORKDIR /home/jenkins

ENTRYPOINT ["/usr/local/bin/jenkins-slave"]
