package com.latticeengines.dante.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;

public class NotionMetadataWrapper {
    @JsonProperty("Key")
    private String key;

    @JsonProperty("Value")
    private NotionMetadata value;

    public NotionMetadataWrapper() {
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public NotionMetadata getValue() {
        return value;
    }

    public void setValue(NotionMetadata value) {
        this.value = value;
    }
}
