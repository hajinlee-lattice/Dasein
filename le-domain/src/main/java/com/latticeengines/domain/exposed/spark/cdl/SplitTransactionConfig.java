package com.latticeengines.domain.exposed.spark.cdl;

import java.io.Serializable;

import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class SplitTransactionConfig extends SparkJobConfig implements Serializable {

    private static final long serialVersionUID = 0L;

    public static final String NAME = "splitTransaction";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 2;
    }
}
