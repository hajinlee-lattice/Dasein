package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStage;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class JourneyStageJobConfig extends SparkJobConfig {

    public static final String NAME = "journeyStageJob";
    private static final long serialVersionUID = -2466292899923863617L;

    /*-
     * inputs
     */

    // latest journey stage table's index in input list
    @JsonProperty
    public Integer masterJourneyStageIdx;

    // timeline master store's index in input list (required)
    @JsonProperty
    public Integer masterAccountTimeLineIdx;

    // diff timeline store's index in input list (required)
    @JsonProperty
    public Integer diffAccountTimeLineIdx;

    @JsonProperty
    public Long currentEpochMilli;

    @JsonProperty
    public String accountTimeLineId;

    @JsonProperty
    public String accountTimeLineVersion;

    // non-default stages
    @JsonProperty
    public List<JourneyStage> journeyStages;

    @JsonProperty
    public JourneyStage defaultStage;

    @Override
    public String getName() {
        return NAME;
    }

    // [ modified master timeline, modified diff timeline, master journey stage
    // store ]
    @Override
    public int getNumTargets() {
        return 3;
    }
}
