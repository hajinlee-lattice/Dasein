package com.latticeengines.domain.exposed.metadata.template;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ImportFieldMapping implements Serializable {

    private static final long serialVersionUID = 127129121395040554L;

    @JsonProperty("fieldName")
    protected String fieldName;

    @JsonProperty("userFieldName")
    protected String userFieldName;

    @JsonProperty("fieldType")
    private UserDefinedType fieldType;

    @JsonProperty("entity")
    private String entity;

    public UserDefinedType getFieldType() {
        return fieldType;
    }

    public void setFieldType(UserDefinedType fieldType) {
        this.fieldType = fieldType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getUserFieldName() {
        return userFieldName;
    }

    public void setUserFieldName(String userFieldName) {
        this.userFieldName = userFieldName;
    }

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }
}
