package com.latticeengines.domain.exposed.pls.frontend;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LatticeSchemaField {

    @JsonProperty
    private String name;

    @JsonProperty
    private RequiredType requiredType;

    @JsonProperty
    private String fieldType;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public void setRequiredType(RequiredType requiredType) {
        this.requiredType = requiredType;
    }

    public RequiredType getRequiredType() {
        return this.requiredType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public String getFieldType() {
        return this.fieldType;
    }
}
