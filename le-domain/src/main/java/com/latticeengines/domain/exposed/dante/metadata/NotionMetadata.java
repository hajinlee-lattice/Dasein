package com.latticeengines.domain.exposed.dante.metadata;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/// <summary>
/// Class for defining metadata about objects (e.g. Account, Lead)
/// </summary>
public class NotionMetadata extends BaseObjectMetadata {

    @JsonProperty("DescriptionKey")
    private String descriptionKey;

    @JsonProperty("DescriptionNameKey")
    private String descriptionNameKey;

    @JsonProperty("Name")
    private String name;

    @JsonProperty("Source")
    private int source;

    @JsonProperty("IsRootNotion")
    private boolean isRootNotion;

    @JsonProperty("Associations")
    private List<Association> associations;

    @JsonProperty("Properties")
    private List<PropertyMetadata> properties;

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
