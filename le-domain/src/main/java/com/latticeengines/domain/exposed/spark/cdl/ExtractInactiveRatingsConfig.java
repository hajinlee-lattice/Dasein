package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class ExtractInactiveRatingsConfig extends SparkJobConfig {

    public static final String NAME = "extractInactiveRatings";

    // bucket name for each input, same order as input list excluding the first (which is the default bucket)
    @JsonProperty("InactiveRatingColumns")
    private List<String> inactiveRatingColumns;

    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public List<String> getInactiveRatingColumns() {
        return inactiveRatingColumns;
    }

    public void setInactiveRatingColumns(List<String> inactiveRatingColumns) {
        this.inactiveRatingColumns = inactiveRatingColumns;
    }

}
