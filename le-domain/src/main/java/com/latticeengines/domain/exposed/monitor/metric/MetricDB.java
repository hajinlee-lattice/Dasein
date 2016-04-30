package com.latticeengines.domain.exposed.monitor.metric;

public enum MetricDB {

    SCORING("Scoring"), LDC_Match("DataCloudMatch"), INSPECTION("Inspection"), TEST_DB("TestDB");

    private final String dbName;

    MetricDB(String dbName) {
        this.dbName = dbName;
    }

    public String getDbName() {
        return dbName;
    }

}
