package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.PlayLaunchSparkContext;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CreateRecommendationConfig extends SparkJobConfig {

    /**
     * 
     */
    private static final long serialVersionUID = 822194165143246260L;

    public static final String NAME = "createRecommendation";

    public static final int NUM_TARGETS = 2;

    @JsonProperty("PlayLaunchSparkContext")
    private PlayLaunchSparkContext playLaunchSparkContext;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return NUM_TARGETS;
    }

    public PlayLaunchSparkContext getPlayLaunchSparkContext() {
        return this.playLaunchSparkContext;
    }

    public void setPlayLaunchSparkContext(PlayLaunchSparkContext playLaunchSparkContext) {
        this.playLaunchSparkContext = playLaunchSparkContext;
    }

}
