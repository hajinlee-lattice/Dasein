package com.latticeengines.dante.metadata;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MetadataDocument {

    @JsonProperty("Notions")
    private List<NotionMetadataWrapper> notions;

    public List<NotionMetadataWrapper> getNotions() {
        return notions;
    }

    public void setNotions(List<NotionMetadataWrapper> notions) {
        this.notions = notions;
    }
}
