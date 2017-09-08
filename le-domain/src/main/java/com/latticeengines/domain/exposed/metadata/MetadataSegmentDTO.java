package com.latticeengines.domain.exposed.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class MetadataSegmentDTO {

    public MetadataSegmentDTO() {
    }

    @JsonProperty("metadataSegment")
    private MetadataSegment metadataSegment;

    @JsonProperty("primaryKey")
    private Long primaryKey;

    public MetadataSegment getMetadataSegment() {
        return this.metadataSegment;
    }

    public void setMetadataSegment(MetadataSegment metadataSegment) {
        this.metadataSegment = metadataSegment;
    }

    public Long getPrimaryKey() {
        return this.primaryKey;
    }

    public void setPrimaryKey(Long primaryKey) {
        this.primaryKey = primaryKey;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
