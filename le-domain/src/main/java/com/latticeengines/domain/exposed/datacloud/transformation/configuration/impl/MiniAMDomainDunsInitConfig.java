package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MiniAMDomainDunsInitConfig extends TransformerConfig {
    @JsonProperty("GoldenInputDataSetDomain")
    private String goldenInputDataSetDomain;

    @JsonProperty("GoldenInputDataSetDuns")
    private String goldenInputDataSetDuns;

    @JsonProperty("OutputDataSetType")
    private String outputDataSetType;

    @JsonProperty("OutputDataSetValue")
    private String outputDataSetValue;

    public String getGoldenInputDataSetDomain() {
        return goldenInputDataSetDomain;
    }

    public void setGoldenInputDataSetDomain(String goldenInputDataSetDomain) {
        this.goldenInputDataSetDomain = goldenInputDataSetDomain;
    }

    public String getGoldenInputDataSetDuns() {
        return goldenInputDataSetDuns;
    }

    public void setGoldenInputDataSetDuns(String goldenInputDataSetDuns) {
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
