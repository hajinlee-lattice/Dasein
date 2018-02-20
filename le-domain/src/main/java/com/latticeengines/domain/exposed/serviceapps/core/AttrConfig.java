package com.latticeengines.domain.exposed.serviceapps.core;


import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class AttrConfig implements Serializable {

    private static final long serialVersionUID = -118514979620559934L;

    @JsonProperty(ColumnMetadataKey.AttrName)
    private String attrName;

    @JsonProperty("Type")
    private AttrType attrType;

    @JsonProperty("Props")
    private Map<String, AttrConfigProp> attrProps;

    public String getAttrName() {
        return attrName;
    }

    public void setAttrName(String attrName) {
        this.attrName = attrName;
    }

    public AttrType getAttrType() {
        return attrType;
    }

    public void setAttrType(AttrType attrType) {
        this.attrType = attrType;
    }

    public Map<String, AttrConfigProp> getAttrProps() {
        return attrProps;
    }

    // Keys must be chosen from the constants in ColumnMetadataKey
    public void setAttrProps(Map<String, AttrConfigProp> attrProps) {
        this.attrProps = attrProps;
    }
}
