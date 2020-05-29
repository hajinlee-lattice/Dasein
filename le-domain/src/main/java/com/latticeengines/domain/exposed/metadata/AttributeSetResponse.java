package com.latticeengines.domain.exposed.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;

public class AttributeSetResponse {

    @JsonProperty("attributeSet")
    private AttributeSet attributeSet;

    @JsonProperty("attrConfigRequest")
    private AttrConfigRequest attrConfigRequest;

    public AttrConfigRequest getAttrConfigRequest() {
        return attrConfigRequest;
    }

    public void setAttrConfigRequest(AttrConfigRequest attrConfigRequest) {
        this.attrConfigRequest = attrConfigRequest;
    }

    public AttributeSet getAttributeSet() {
        return attributeSet;
    }

    public void setAttributeSet(AttributeSet attributeSet) {
        this.attributeSet = attributeSet;
    }
}
