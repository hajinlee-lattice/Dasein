package com.latticeengines.domain.exposed.spark.cdl;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class ActivityAlertJobConfig extends SparkJobConfig {
    public static final String NAME = "activityAlertJob";

    // timeline master store's index in input list (required)
    @NotNull
    @JsonProperty
    public Integer masterAccountTimeLineIdx;

    @JsonProperty
    public Integer masterAlertIdx;

    @JsonProperty
    public Map<String, Long> alertNameToQualificationPeriodDays = new HashMap<>();

    @JsonProperty
    public Map<String, String> alertNameToAlertCategory = new HashMap<>();

    @JsonProperty
    public Long currentEpochMilli;

    @Override
    public int getNumTargets() {
        // [ merged alerts, new alerts generated in current job ]
        return 2;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
