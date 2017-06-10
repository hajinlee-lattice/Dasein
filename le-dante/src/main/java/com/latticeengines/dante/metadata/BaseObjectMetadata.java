package com.latticeengines.dante.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BaseObjectMetadata {
    /// <summary>

    /// An object can either be a notion or a property
    /// </summary>
    @JsonProperty("Name")
    private String name;

    /// <summary>
    /// Display name key in a resource dictionary for localization
    /// </summary>
    @JsonProperty("DisplayNameKey")
    private String displayNameKey;

    /// <summary>
    /// Description name key in a resource dictionary for localization
    /// </summary>
    @JsonProperty("DescriptionKey")
    private String descriptionKey;

    /// <summary>
    /// This indicates what source added the object, not what modified it.
    /// </summary>
    @JsonProperty("Source")
    private MetadataSource source;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDisplayNameKey() {
        return displayNameKey;
    }

    public void setDisplayNameKey(String displayNameKey) {
        this.displayNameKey = displayNameKey;
    }

    public String getDescriptionKey() {
        return descriptionKey;
    }

    public void setDescriptionKey(String descriptionKey) {
        this.descriptionKey = descriptionKey;
    }

    public MetadataSource getSource() {
        return source;
    }

    public void setSource(MetadataSource source) {
        this.source = source;
    }
}
