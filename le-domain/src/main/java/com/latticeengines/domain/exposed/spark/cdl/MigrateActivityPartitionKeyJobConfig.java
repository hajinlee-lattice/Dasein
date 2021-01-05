package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class MigrateActivityPartitionKeyJobConfig extends SparkJobConfig {

    public static final String NAME = "migrateActivityPartitionKeyJobConfig";
    private static final long serialVersionUID = 0L;

    @JsonProperty
    // streamId -> [legacy partition keys] same order as input tables
    public SparkIOMetadataWrapper inputMetadata;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        // each stream should have two tables to migrate: rawStream and dailyStore
        return inputMetadata.getMetadata().size() * 2;
    }
}
