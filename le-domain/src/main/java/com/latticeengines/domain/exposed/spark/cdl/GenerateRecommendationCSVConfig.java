package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.GenerateRecommendationCSVContext;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class GenerateRecommendationCSVConfig extends SparkJobConfig {

    private static final long serialVersionUID = -8660768334611758742L;

    public static final String NAME = "generateRecommendationCSV";

    @JsonProperty("TargetNums")
    private int targetNums;

    @JsonProperty("GenerateRecommendationCSVContext")
    private GenerateRecommendationCSVContext generateRecommendationCSVContext;

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

    public GenerateRecommendationCSVContext getGenerateRecommendationCSVContext() {
        return generateRecommendationCSVContext;
    }

    public void setGenerateRecommendationCSVContext(GenerateRecommendationCSVContext generateRecommendationCSVContext) {
        this.generateRecommendationCSVContext = generateRecommendationCSVContext;
    }
}
