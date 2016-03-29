package com.latticeengines.dataflow.exposed.builder.common;

import org.apache.avro.Schema;

public enum AggregationType {
    MAX, //
    MIN, //
    SUM(new FieldMetadata(Schema.Type.DOUBLE, Double.class, null, null)), //
    COUNT(new FieldMetadata(Schema.Type.LONG, Long.class, null, null)), //
    AVG(new FieldMetadata(Schema.Type.DOUBLE, Double.class, null, null)), //
    FIRST, //
    LAST;

    private FieldMetadata fieldMetadata;

    AggregationType() {
        this(null);
    }

    AggregationType(FieldMetadata fieldMetadata) {
        this.fieldMetadata = fieldMetadata;
    }

    public FieldMetadata getFieldMetadata() {
        return fieldMetadata;
    }

}