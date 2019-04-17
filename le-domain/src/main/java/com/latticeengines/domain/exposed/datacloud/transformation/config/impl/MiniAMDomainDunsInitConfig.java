package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MiniAMDomainDunsInitConfig extends TransformerConfig {
    @JsonProperty("GoldenInputDataSetDomain")
    private Map<String, String> goldenInputDataSetDomain;

    @JsonProperty("GoldenInputDataSetDuns")
    private Map<String, String> goldenInputDataSetDuns;

    @JsonProperty("OutputDataSetType")
    private String outputDataSetType;

    @JsonProperty("OutputDataSetValue")
    private String outputDataSetValue;

    public Map<String, String> getGoldenInputDataSetDomain() {
        return goldenInputDataSetDomain;
    }

    public void setGoldenInputDataSetDomain(Map<String, String> goldenInputDataSetDomain) {
        this.goldenInputDataSetDomain = goldenInputDataSetDomain;
    }

    public Map<String, String> getGoldenInputDataSetDuns() {
        return goldenInputDataSetDuns;
    }

    public void setGoldenInputDataSetDuns(Map<String, String> goldenInputDataSetDuns) {
        this.goldenInputDataSetDuns = goldenInputDataSetDuns;
    }

    public String getOutputDataSetType() {
        return outputDataSetType;
    }

    public void setOutputDataSetType(String outputDataSetType) {
        this.outputDataSetType = outputDataSetType;
    }

    public String getOutputDataSetValue() {
        return outputDataSetValue;
    }

    public void setOutputDataSetValue(String outputDataSetValue) {
        this.outputDataSetValue = outputDataSetValue;
    }

}
