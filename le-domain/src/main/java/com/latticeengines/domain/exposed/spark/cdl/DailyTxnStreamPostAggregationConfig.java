package com.latticeengines.domain.exposed.spark.cdl;

import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class DailyTxnStreamPostAggregationConfig extends SparkJobConfig {
    public static final String NAME = "dailyTxnStreamPostAggregationConfig";

    @Override
    public String getName() {
        return NAME;
    }
}
