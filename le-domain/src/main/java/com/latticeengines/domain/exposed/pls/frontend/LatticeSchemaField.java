package com.latticeengines.domain.exposed.pls.frontend;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;

public class LatticeSchemaField {

    @JsonProperty
    private String name;

    @JsonProperty
    private RequiredType requiredType;

    @JsonProperty
    private UserDefinedType fieldType;

    @JsonProperty
    private String requiredIfNoField;

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

    public void setFieldType(UserDefinedType fieldType) {
        this.fieldType = fieldType;
    }

    public UserDefinedType getFieldType() {
        return this.fieldType;
    }

    public void setRequiredIfNoField(String requiredIfNoField) {
        this.requiredIfNoField = requiredIfNoField;
    }

    public String getRequiredIfNoField() {
        return this.requiredIfNoField;
    }
}
