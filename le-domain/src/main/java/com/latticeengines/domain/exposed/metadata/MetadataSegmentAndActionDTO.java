package com.latticeengines.domain.exposed.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.Action;

public class MetadataSegmentAndActionDTO {

    @JsonProperty("metadataSegment")
    private MetadataSegment metadataSegment;

    @JsonProperty("action")
    private Action action;

    public MetadataSegmentAndActionDTO() {
    }

    public MetadataSegmentAndActionDTO(MetadataSegment metadataSegment, Action action) {
        this.metadataSegment = metadataSegment;
        this.action = action;
    }

    public MetadataSegment getMetadataSegment() {
        return this.metadataSegment;
    }

    public void setMetadataSegment(MetadataSegment metadataSegment) {
        this.metadataSegment = metadataSegment;
    }

    public Action getAction() {
        return this.action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
