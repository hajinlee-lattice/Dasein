package com.latticeengines.domain.exposed.pls.frontend;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;

public class FieldMapping {

    @JsonProperty
    private String userField;

    @JsonProperty
    private String mappedField;

    @JsonProperty
    private UserDefinedType fieldType;

    @JsonProperty
    private CDLExternalSystemType cdlExternalSystemType = null;

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

    public CDLExternalSystemType getCdlExternalSystemType() {
        return cdlExternalSystemType;
    }

    public void setCdlExternalSystemType(CDLExternalSystemType cdlExternalSystemType) {
        this.cdlExternalSystemType = cdlExternalSystemType;
    }

    public void setMappedToLatticeField(boolean mappedToLatticeField) {
        this.mappedToLatticeField = mappedToLatticeField;
    }

    public boolean isMappedToLatticeField() {
        return this.mappedToLatticeField;
    }
    
    public String toString(){
        return JsonUtils.serialize(this);
    }
}
