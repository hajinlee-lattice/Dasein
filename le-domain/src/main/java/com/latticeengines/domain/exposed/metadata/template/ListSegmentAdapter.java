package com.latticeengines.domain.exposed.metadata.template;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class ListSegmentAdapter extends CSVAdaptor {

    public static final String NAME = "listSegment";

    @JsonProperty("externalSystem")
    private String externalSystem;

    @JsonProperty("externalSegmentId")
    private String externalSegmentId;

    @Override
    public String getName() {
        return NAME;
    }
}
