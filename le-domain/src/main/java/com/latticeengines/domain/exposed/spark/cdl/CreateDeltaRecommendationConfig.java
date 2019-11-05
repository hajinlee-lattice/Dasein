package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.DeltaCampaignLaunchSparkContext;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CreateDeltaRecommendationConfig extends SparkJobConfig {

    /**
     * 
     */
    private static final long serialVersionUID = 822194165143246260L;

    public static final String NAME = "createDeltaRecommendation";

    private int targetNums;

    @JsonProperty("DeltaCampaignLaunchSparkContext")
    private DeltaCampaignLaunchSparkContext deltaCampaignLaunchSparkContext;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return targetNums;
    }

    public void setTargetNums(int targetNums) {
        this.targetNums = targetNums;
    }

    public DeltaCampaignLaunchSparkContext getDeltaCampaignLaunchSparkContext() {
        return this.deltaCampaignLaunchSparkContext;
    }

    public void setDeltaCampaignLaunchSparkContext(DeltaCampaignLaunchSparkContext deltaCampaignLaunchSparkContext) {
        this.deltaCampaignLaunchSparkContext = deltaCampaignLaunchSparkContext;
    }

}
