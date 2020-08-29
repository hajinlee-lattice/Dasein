package com.latticeengines.domain.exposed.spark.cdl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TransformTxnStreamConfig extends SparkJobConfig {
    public static final String NAME = "transformTxnStreamConfig";
    private static final long serialVersionUID = 0L;

    @JsonProperty
    public List<String> compositeSrc = new ArrayList<>();

    @JsonProperty
    public Map<String, String> renameMapping = new HashMap<>(); // stream col -> target col

    @JsonProperty
    public List<String> targetColumns = new ArrayList<>(); // all columns in output schema

    @JsonProperty
    public List<String> inputPeriods = new ArrayList<>(); // same order as inputs,

    @JsonProperty
    public String partitionKey;

    @Override
    public String getName() {
        return NAME;
    }
}
