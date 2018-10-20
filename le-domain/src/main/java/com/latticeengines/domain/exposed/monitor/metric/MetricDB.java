package com.latticeengines.domain.exposed.monitor.metric;

public enum MetricDB {

    LEYARN("LEYarn"), SCORING("Scoring"), LDC_Match("DataCloudMatch"), INSPECTION(
            "Inspection"), MODEL_QUALITY(
                    "ModelQuality"), TEST_DB("TestDB"), PLAYMAKER("Playmaker"), ULYSSES("Ulysses");

    private final String dbName;

    MetricDB(String dbName) {
        this.dbName = dbName;
    }

    public String getDbName() {
        return dbName;
    }

}
