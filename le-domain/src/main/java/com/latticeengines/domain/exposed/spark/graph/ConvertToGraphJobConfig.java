package com.latticeengines.domain.exposed.spark.graph;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class ConvertToGraphJobConfig extends SparkJobConfig {

    public static final String NAME = "convertToGraphJob";

    @JsonProperty("InputDescriptors")
    private List<Map<String, String>> inputDescriptors;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 2;
    }

    public List<Map<String, String>> getInputDescriptors() {
        return inputDescriptors;
    }

    public void setInputDescriptors(List<Map<String, String>> inputDescriptors) {
        this.inputDescriptors = inputDescriptors;
    }
}
