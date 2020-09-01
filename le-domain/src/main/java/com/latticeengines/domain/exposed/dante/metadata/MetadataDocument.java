package com.latticeengines.domain.exposed.dante.metadata;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

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
