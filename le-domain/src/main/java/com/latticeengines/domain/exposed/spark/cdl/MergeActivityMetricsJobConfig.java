package com.latticeengines.domain.exposed.spark.cdl;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class MergeActivityMetricsJobConfig extends SparkJobConfig implements Serializable {

    private static final long serialVersionUID = 0L;

    public static final String NAME = "mergeActivityMetricsJobConfig";

    @JsonProperty("mergedTableLabels")
    // entity_servingEntity
    public List<String> mergedTableLabels;

    @JsonProperty("inputMetadata")
    // describe mergedTableName -> dataframes to merge
    // labels: groupId
    public ActivityStoreSparkIOMetadata inputMetadata;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return mergedTableLabels.size();
    }
}
