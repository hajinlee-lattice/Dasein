package com.latticeengines.dante.metadata;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/// <summary>
/// Class for defining metadata about objects (e.g. Account, Lead)
/// </summary>
@JsonIgnoreProperties({ "Associations" })
public class NotionMetadata extends BaseObjectMetadata {
    @JsonProperty("Properties")
    private List<PropertyMetadata> properties;

    @JsonProperty("IsRootNotion")
    private boolean isRootNotion;

    public List<PropertyMetadata> getProperties() {
        return properties;
    }

    public void setProperties(List<PropertyMetadata> properties) {
        this.properties = properties;
    }

    public boolean isRootNotion() {
        return isRootNotion;
    }

    public void setRootNotion(boolean rootNotion) {
        isRootNotion = rootNotion;
    }
}
