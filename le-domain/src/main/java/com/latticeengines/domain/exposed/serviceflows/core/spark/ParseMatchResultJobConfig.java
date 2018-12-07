package com.latticeengines.domain.exposed.serviceflows.core.spark;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ParseMatchResultJobConfig extends WorkflowSparkJobConfig {

    public static final String NAME = "parseMatchResult";

    @JsonProperty
    public List<String> sourceColumns;

    @JsonProperty
    public boolean excludeDataCloudAttrs;

    @JsonProperty
    public boolean keepLid;

    @JsonProperty
    public String idColumnName;

    @JsonProperty
    public String matchGroupId;

    @JsonProperty
    public boolean joinInternalId;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

}
