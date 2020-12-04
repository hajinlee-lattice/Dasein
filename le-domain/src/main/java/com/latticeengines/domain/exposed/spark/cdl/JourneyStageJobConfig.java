package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
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

    // timeline master store's index in input list
    @NotNull
    @JsonProperty
    public Integer masterAccountTimeLineIdx;

    // diff timeline store's index in input list
    @NotNull
    @JsonProperty
    public Integer diffAccountTimeLineIdx;

    @NotNull
    @JsonProperty
    public Long earliestBackfillEpochMilli;

    @NotNull
    @JsonProperty
    public Long currentEpochMilli;

    @NotNull
    @JsonProperty
    public Long backfillStepInDays;

    @NotNull
    @JsonProperty
    public String accountTimeLineId;

    @NotNull
    @JsonProperty
    public String accountTimeLineVersion;

    // non-default stages
    @NotNull
    @JsonProperty
    public List<JourneyStage> journeyStages;

    @NotNull
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
