package com.latticeengines.domain.exposed.monitor.metric;

public enum MetricDB {

    LEYARN("LEYarn"), //
    SCORING("Scoring"), //
    LDC_Match("DataCloudMatch"), //
    INSPECTION("Inspection"), //
    MODEL_QUALITY("ModelQuality"), //
    PLAYMAKER("Playmaker"), //
    ULYSSES("Ulysses"), //
    TEST_DB("TestDB");

    private final String dbName;

    MetricDB(String dbName) {
        this.dbName = dbName;
    }

    public String getDbName() {
        return dbName;
    }

}
