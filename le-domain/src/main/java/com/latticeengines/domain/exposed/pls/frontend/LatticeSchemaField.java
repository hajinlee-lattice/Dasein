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

    @JsonProperty
    private Boolean fromExistingTemplate = false;

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public RequiredType getRequiredType() {
        return this.requiredType;
    }

    public void setRequiredType(RequiredType requiredType) {
        this.requiredType = requiredType;
    }

    public UserDefinedType getFieldType() {
        return this.fieldType;
    }

    public void setFieldType(UserDefinedType fieldType) {
        this.fieldType = fieldType;
    }

    public String getRequiredIfNoField() {
        return this.requiredIfNoField;
    }

    public void setRequiredIfNoField(String requiredIfNoField) {
        this.requiredIfNoField = requiredIfNoField;
    }

    public Boolean getFromExistingTemplate() {
        return fromExistingTemplate;
    }

    public void setFromExistingTemplate(Boolean fromExistingTemplate) {
        this.fromExistingTemplate = fromExistingTemplate;
    }
}
