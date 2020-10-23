package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class GenerateIntentAlertArtifactsConfig extends SparkJobConfig {

    public static final String NAME = "generateIntentAlertArtifacts";

    @JsonProperty("DimensionMetadata")
    private DimensionMetadata dimensionMetadata;

    @JsonProperty("SelectedAttributes")
    private List<String> selectedAttributes;

    public DimensionMetadata getDimensionMetadata() {
        return dimensionMetadata;
    }

    public void setDimensionMetadata(DimensionMetadata dimensionMetadata) {
        this.dimensionMetadata = dimensionMetadata;
    }

    public List<String> getSelectedAttributes() {
        return selectedAttributes;
    }

    public void setSelectedAttributes(List<String> selectedAttributes) {
        this.selectedAttributes = selectedAttributes;
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
