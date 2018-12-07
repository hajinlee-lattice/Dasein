package com.latticeengines.domain.exposed.spark;

public enum SparkInterpreter {

    Scala("spark"), Python("pyspark");

    private final String kind;

    SparkInterpreter(String kind) {
        this.kind = kind;
    }

    public String getKind() {
        return kind;
    }
}
