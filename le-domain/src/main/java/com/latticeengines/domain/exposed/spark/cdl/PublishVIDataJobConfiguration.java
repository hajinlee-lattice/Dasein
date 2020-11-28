package com.latticeengines.domain.exposed.spark.cdl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class PublishVIDataJobConfiguration extends SparkJobConfig implements Serializable {

    private static final long serialVersionUID = 0L;
    public static final String NAME = "publishVIDataJob";

    @JsonProperty
    public Map<String, Integer> inputIdx = new HashMap<>();
    @JsonProperty
    public Integer latticeAccountTableIdx;
    @JsonProperty
    public List<String> selectedAttributes;
    @JsonProperty
    public List<String> webVisitAttributes;
    @JsonProperty
    public List<String> latticeAccountAttributes;
    @JsonProperty
    public Map<String, String> esConfigs = new HashMap<>();

    @Override
    public String getName() {
        return NAME;
    }
}
