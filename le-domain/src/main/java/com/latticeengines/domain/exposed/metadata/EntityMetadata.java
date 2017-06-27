package com.latticeengines.domain.exposed.metadata;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class EntityMetadata {

    @JsonProperty("Entity")
    private BusinessEntity entity;

    @JsonProperty("Attributes")
    private List<ColumnMetadata> attributes;

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public List<ColumnMetadata> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<ColumnMetadata> attributes) {
        this.attributes = attributes;
    }
}
