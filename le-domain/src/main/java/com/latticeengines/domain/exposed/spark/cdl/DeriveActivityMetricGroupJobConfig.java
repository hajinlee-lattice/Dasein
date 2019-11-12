package com.latticeengines.domain.exposed.spark.cdl;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class DeriveActivityMetricGroupJobConfig extends SparkJobConfig implements Serializable {

    private static final long serialVersionUID = 0L;

    public static final String NAME = "DeriveActivityMetricGroupJobConfig";

    @JsonProperty("groups")
    public List<ActivityMetricsGroup> activityMetricsGroups;

    @JsonProperty("inputMetadata")
    public ActivityStoreSparkIOMetadata inputMetadata; // describe streamId -> period stores

    @JsonProperty("evaluationDate")
    // period -> current period Id
    public String evaluationDate;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return activityMetricsGroups.size();
    }
}
