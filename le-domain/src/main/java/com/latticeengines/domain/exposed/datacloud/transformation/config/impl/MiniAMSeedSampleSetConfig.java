package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MiniAMSeedSampleSetConfig extends TransformerConfig {
    @JsonProperty("SampledSetDomain")
    private String sampledSetDomain;
    @JsonProperty("SampledSetDuns")
    private String sampledSetDuns;
    @JsonProperty("MiniDataSetType")
    private String miniDataSetType;
    @JsonProperty("MiniDataSetValue")
    private String miniDataSetValue;
    @JsonProperty("KeyIdentifier")
    private List<String> keyIdentifier;

    public String getSampledSetDomain() {
        return sampledSetDomain;
    }

    public void setSampledSetDomain(String sampledSetDomain) {
        this.sampledSetDomain = sampledSetDomain;
    }

    public String getSampledSetDuns() {
        return sampledSetDuns;
    }

    public void setSampledSetDuns(String sampledSetDuns) {
        this.sampledSetDuns = sampledSetDuns;
    }

    public String getMiniDataSetType() {
        return miniDataSetType;
    }

    public void setMiniDataSetType(String miniDataSetType) {
        this.miniDataSetType = miniDataSetType;
    }

    public String getMiniDataSetValue() {
        return miniDataSetValue;
    }

    public void setMiniDataSetValue(String miniDataSetValue) {
        this.miniDataSetValue = miniDataSetValue;
    }

    public List<String> getKeyIdentifier() {
        return keyIdentifier;
    }

    public void setKeyIdentifier(List<String> keyIdentifier) {
        this.keyIdentifier = keyIdentifier;
    }
}
