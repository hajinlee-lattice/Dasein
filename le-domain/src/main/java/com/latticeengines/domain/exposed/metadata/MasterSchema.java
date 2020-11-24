package com.latticeengines.domain.exposed.metadata;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class MasterSchema {
    @JsonProperty("attributes")
    private List<ColumnField> attributes;

    @JsonProperty("primaryKey")
    private List<String> primaryKey;

    public List<ColumnField> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<ColumnField> attributes) {
        this.attributes = attributes;
    }

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(List<String> primaryKey) {
        this.primaryKey = primaryKey;
    }
}
