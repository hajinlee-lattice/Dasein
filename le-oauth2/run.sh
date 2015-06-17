#mvn -Pfunctional -DADMIN_PROPDIR=conf/env/dev jetty:run
mvn -Pfunctional -DPLAYMAKER_PROPDIR=conf/env/dev -DargLine="" -Djavax.net.ssl.trustStore=certificates/ga209-ldap.dev.lattice.local.jks jetty:run
