package com.latticeengines.domain.exposed.spark.cdl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class MergeTimeSeriesDeleteDataConfig extends SparkJobConfig {

    public static final String NAME = "mergeTimeSeriesDeleteData";
    private static final long serialVersionUID = -6214087850234080951L;

    @JsonProperty("JoinKey")
    public String joinKey;

    // input idx -> [ start time, end time ] (time could be epoch timestamp or day
    // period)
    @JsonProperty("TimeRanges")
    public Map<Integer, List<Long>> timeRanges = new HashMap<>();

    @Override
    public String getName() {
        return NAME;
    }
}
