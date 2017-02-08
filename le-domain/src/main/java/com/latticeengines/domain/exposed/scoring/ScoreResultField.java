package com.latticeengines.domain.exposed.scoring;

import org.apache.avro.Schema.Type;

public enum ScoreResultField {

    Percentile(Type.INT.name(), "Score", Integer.class.getSimpleName()), //
    RawScore(Type.DOUBLE.name(), "RawScore", Double.class.getSimpleName()), //
    Rating(Type.STRING.name(), "Rating", String.class.getSimpleName());

    public String physicalDataType;

    public String displayName;

    public String sourceLogicalDataType;

    ScoreResultField(String physicalDataType, String displayName, String sourceLogicalDataType) {
        this.physicalDataType = physicalDataType;
        this.sourceLogicalDataType = sourceLogicalDataType;
        this.displayName = displayName;
    }

}
