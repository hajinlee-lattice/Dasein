package com.latticeengines.domain.exposed.dante.metadata;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

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
