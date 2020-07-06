package com.latticeengines.domain.exposed.spark.common;

import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class GetRowChangesConfig extends SparkJobConfig {

    public static final String NAME = "getRowChanges";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 2;
    }

}
