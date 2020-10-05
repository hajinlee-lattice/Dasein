package com.latticeengines.domain.exposed.spark.cdl;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class GenerateIntentAlertArtifactsConfig extends SparkJobConfig {

    public static final String NAME = "generateIntentAlertArtifacts";

    @JsonProperty("DimensionMetadata")
    private DimensionMetadata dimensionMetadata;

    @JsonProperty("outputColumns")
    private Set<String> outputColumns;

    public DimensionMetadata getDimensionMetadata() {
        return dimensionMetadata;
    }

    public void setDimensionMetadata(DimensionMetadata dimensionMetadata) {
        this.dimensionMetadata = dimensionMetadata;
    }

    public Set<String> getOutputColumns() {
        return outputColumns;
    }

    public void setOutputColumns(Set<String> outputColumns) {
        this.outputColumns = outputColumns;
    }

    @Override
    public int getNumTargets() {
        // [new accounts show intent in current week (not last week), all accounts show
        // intent in current week]
        return 2;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
