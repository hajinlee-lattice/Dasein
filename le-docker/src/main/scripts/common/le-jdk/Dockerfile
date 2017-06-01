FROM latticeengines/centos
RUN yum install -y unzip wget && yum clean all
RUN mkdir /usr/java && chmod 755 /usr/java
WORKDIR /usr/java
RUN wget -q http://10.41.1.10/tars/jdk-8u101-linux-x64.tar.gz  -O jdk-8u101-linux-x64.tar.gz \
    && tar xf jdk-8u101-linux-x64.tar.gz \
    && rm -f jdk-8u101-linux-x64.tar.gz \
    && wget -q http://10.41.1.10/tars/jce_policy-8.zip -O jce_policy-8.zip  \
    && unzip jce_policy-8.zip \
    && cp -a UnlimitedJCEPolicyJDK8/*  /usr/java/jdk1.8.0_101/jre/lib/security \
    && rm -rf jce_policy-8.zip  UnlimitedJCEPolicyJDK8 \
    && rm -f /usr/java/default /usr/java/latest \
    && ln -s /usr/java/jdk1.8.0_101 /usr/java/latest \
    && ln -s /usr/java/latest /usr/java/default  \
    && echo 'export JAVA_HOME=/usr/java/default' >> /etc/profile \
    && chown -R root.root /usr/java
RUN update-alternatives --install /usr/bin/java java /usr/java/jdk1.8.0_101/bin/java  100 \
    && update-alternatives --set java /usr/java/jdk1.8.0_101/bin/java \
    && alternatives  --install /bin/java binjava /usr/java/latest/bin/java 1 \
    && alternatives  --install /usr/bin/java java /usr/java/latest/bin/java 1 \
    && alternatives  --install /usr/bin/javac  javac  /usr/java/latest/bin/javac 1 \
    && alternatives  --install /usr/bin/jar  jar  /usr/java/latest/bin/jar 1 \
    && alternatives  --install /usr/bin/jps  jps  /usr/java/latest/bin/jps 1 \
    && alternatives --set binjava /usr/java/latest/bin/java \
    && alternatives --set java /usr/java/latest/bin/java \
    && alternatives --set javac /usr/java/latest/bin/javac \
    && alternatives --set jar /usr/java/latest/bin/jar \
    && alternatives --set jps /usr/java/latest/bin/jps
ENV JAVA_HOME /usr/java/default
