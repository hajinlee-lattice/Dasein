package com.latticeengines.domain.exposed.metadata;

import org.apache.avro.Schema;

import com.latticeengines.domain.exposed.scoringapi.FieldType;

public enum UserDefinedType {

    TEXT(Schema.Type.STRING, FieldType.STRING), //
    NUMBER(Schema.Type.DOUBLE, FieldType.FLOAT), //
    BOOLEAN(Schema.Type.BOOLEAN, FieldType.BOOLEAN);
    
    private Schema.Type avroType;
    private FieldType fieldType;
    
    UserDefinedType(Schema.Type avroType, FieldType fieldType) {
        this.avroType = avroType;
        this.fieldType = fieldType;
    }
    
    public Schema.Type getAvroType() {
        return avroType;
    }
    
    public FieldType getFieldType() {
        return fieldType;
    }
    
    public Object cast(String value) {
        if (avroType == Schema.Type.DOUBLE) {
            return Double.valueOf(value);
        } else if (avroType == Schema.Type.BOOLEAN) {
            return Boolean.valueOf(value);
        }
        return value;
    }
}
