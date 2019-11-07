package com.latticeengines.domain.exposed.spark.cdl;

import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class DeriveActivityMetricGroupJobConfig extends SparkJobConfig implements Serializable {

    private static final long serialVersionUID = 0L;

    public static final String NAME = "DeriveActivityMetricGroupJobConfig";

    @JsonProperty("metricsGroupConfigs")
    public ActivityMetricsGroup activityMetricsGroup;

    @JsonProperty("periodStoreMap")
    // period -> idx of dataframe list from input
    public Map<String, Integer> periodStoreMap;

    @JsonProperty("evaluationDate")
    // period -> current period Id
    public String evaluationDate;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }
}
