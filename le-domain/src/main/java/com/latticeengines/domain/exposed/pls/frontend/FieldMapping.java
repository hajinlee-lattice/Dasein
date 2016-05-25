package com.latticeengines.domain.exposed.pls.frontend;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;

public class FieldMapping {

    @JsonProperty
    private String userField;

    @JsonProperty
    private String mappedField;

    @JsonProperty
    private UserDefinedType fieldType;

    @JsonProperty
    private boolean mappedToLatticeField;

    public void setUserField(String userField) {
        this.userField = userField;
    }

    public String getUserField() {
        return this.userField;
    }

    public void setMappedField(String mappedField) {
        this.mappedField = mappedField;
    }

    public String getMappedField() {
        return this.mappedField;
    }

    public void setFieldType(UserDefinedType fieldType) {
        this.fieldType = fieldType;
    }

    public UserDefinedType getFieldType() {
        return this.fieldType;
    }

    public void setMappedToLatticeField(boolean mappedToLatticeField) {
        this.mappedToLatticeField = mappedToLatticeField;
    }

    public boolean isMappedToLatticeField() {
        return this.mappedToLatticeField;
    }
}
