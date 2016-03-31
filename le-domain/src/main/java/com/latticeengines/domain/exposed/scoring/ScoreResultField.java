package com.latticeengines.domain.exposed.scoring;

import org.apache.avro.Schema.Type;

public enum ScoreResultField {

    Percentile(Type.INT.name(), Integer.class.getSimpleName()), //
    RawScore(Type.DOUBLE.name(), Double.class.getSimpleName()); //

    public String physicalDataType;

    public String sourceLogicalDataType;

    ScoreResultField(String physicalDataType, String sourceLogicalDataType) {
        this.physicalDataType = physicalDataType;
        this.sourceLogicalDataType = sourceLogicalDataType;
    }

}
