package com.latticeengines.domain.exposed.spark.cdl;

import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class PeriodTxnStreamPostAggregationConfig extends SparkJobConfig {
    public static final String NAME = "periodTxnStreamPostAggregationConfig";

    @Override
    public String getName() {
        return NAME;
    }
}
