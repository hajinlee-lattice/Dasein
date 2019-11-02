package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class AppendRawStreamConfig extends SparkJobConfig {

    public static final String NAME = "appendRawActivityStream";

    @JsonProperty
    public Long currentEpochMilli;

    // null if no imports
    @JsonProperty
    public Integer matchedRawStreamInputIdx;

    // null if no master store
    @JsonProperty
    public Integer masterInputIdx;

    @JsonProperty
    public String dateAttr;

    // null means keep everything
    @JsonProperty
    public Integer retentionDays;

    @Override
    public String getName() {
        return NAME;
    }
}
