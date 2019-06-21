package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class MergeRuleRatingsConfig extends SparkJobConfig {

    public static final String NAME = "mergeRuleRatings";

    // bucket name for each input, same order as input list excluding the first (which is the default bucket)
    @JsonProperty("BucketNames")
    private List<String> bucketNames;

    @JsonProperty("DefaultBucketName")
    private String defaultBucketName;

    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public List<String> getBucketNames() {
        return bucketNames;
    }

    public void setBucketNames(List<String> bucketNames) {
        this.bucketNames = bucketNames;
    }

    public String getDefaultBucketName() {
        return defaultBucketName;
    }

    public void setDefaultBucketName(String defaultBucketName) {
        this.defaultBucketName = defaultBucketName;
    }
}
