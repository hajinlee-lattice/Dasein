package com.latticeengines.domain.exposed.metadata;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ColumnField {
    @JsonProperty("attrName")
    private String attrName;

    @JsonProperty("displayName")
    private String displayName;

    @JsonProperty("entity")
    private BusinessEntity entity;

    public String getAttrName() {
        return attrName;
    }

    public void setAttrName(String attrName) {
        this.attrName = attrName;
    }

    public ColumnMetadata toColumnMetadata() {
        ColumnMetadata columnMetadata = new ColumnMetadata();
        columnMetadata.setEntity(entity);
        columnMetadata.setAttrName(attrName);
        columnMetadata.setDisplayName(displayName);
        return columnMetadata;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }
}
