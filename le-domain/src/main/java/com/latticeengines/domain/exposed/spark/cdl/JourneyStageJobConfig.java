package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStage;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class JourneyStageJobConfig extends SparkJobConfig {

    public static final String NAME = "journeyStageJob";
    private static final long serialVersionUID = -2466292899923863617L;

    // TODO comments

    @JsonProperty
    public Integer masterAccountStoreIdx;

    @JsonProperty
    public Integer masterAccountTimeLineIdx;

    @JsonProperty
    public Integer diffAccountTimeLineIdx;

    @JsonProperty
    public Long currentEpochMilli;

    @JsonProperty
    public List<JourneyStage> journeyStages;

    @Override
    public String getName() {
        return NAME;
    }

    // TODO comments
    @Override
    public int getNumTargets() {
        return diffAccountTimeLineIdx == null ? 1 : 2;
    }
}
