package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class MergeScoringTargetsConfig extends SparkJobConfig {

    public static final String NAME = "mergeScoringTargets";

    @JsonProperty("Containers")
    private List<RatingModelContainer> containers;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public List<RatingModelContainer> getContainers() {
        return containers;
    }

    public void setContainers(List<RatingModelContainer> containers) {
        this.containers = containers;
    }

}
