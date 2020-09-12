# Bootstrap InfluxDB

We disabled the capability of automatically creating 
influxdb databases and retention policies from code, 
therefore we need to manually create the db objects.

First, connect to the influxdb as root user.
Then run following commands

## Create databases

check MetricDB.java

- create database DataCloudMatch;
- create database Inspection;
- create database Playmaker;
- create database Ulysses;
- create database LEYarn;

## Add retention policy

check RetentionPolicyImpl.java

- CREATE RETENTION POLICY "OneHour" ON "Inspection" DURATION 1h REPLICATION 1;
- CREATE RETENTION POLICY "OneDay" ON "Inspection" DURATION 24h REPLICATION 1;
- CREATE RETENTION POLICY "OneWeek" ON "Inspection" DURATION 1w REPLICATION 1;
- CREATE RETENTION POLICY "OneMonth" ON "Inspection" DURATION 4w REPLICATION 1;
- CREATE RETENTION POLICY "TwoQuarters" ON "Inspection" DURATION 26w REPLICATION 1;

- CREATE RETENTION POLICY "OneHour" ON "DataCloudMatch" DURATION 1h REPLICATION 1;
- CREATE RETENTION POLICY "OneDay" ON "DataCloudMatch" DURATION 24h REPLICATION 1;
- CREATE RETENTION POLICY "OneWeek" ON "DataCloudMatch" DURATION 1w REPLICATION 1;
- CREATE RETENTION POLICY "OneMonth" ON "DataCloudMatch" DURATION 4w REPLICATION 1;
- CREATE RETENTION POLICY "TwoQuarters" ON "DataCloudMatch" DURATION 26w REPLICATION 1;

- CREATE RETENTION POLICY "OneHour" ON "LEYarn" DURATION 1h REPLICATION 1;
- CREATE RETENTION POLICY "OneDay" ON "LEYarn" DURATION 24h REPLICATION 1;
- CREATE RETENTION POLICY "OneWeek" ON "LEYarn" DURATION 1w REPLICATION 1;
- CREATE RETENTION POLICY "OneMonth" ON "LEYarn" DURATION 4w REPLICATION 1;
- CREATE RETENTION POLICY "TwoQuarters" ON "LEYarn" DURATION 26w REPLICATION 1;

