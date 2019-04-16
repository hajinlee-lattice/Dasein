package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.step.IterativeStepConfig;

public class MiniAMDomainDunsConfig extends TransformerConfig {
    @JsonProperty("DnbInputDataSetDomain")
    private String dnbInputDataSetDomain;
    @JsonProperty("DnbInputDataSetDuns")
    private String dnbInputDataSetDuns;
    @JsonProperty("DnbInputDataSetGU")
    private String dnbInputDataSetGU;
    @JsonProperty("DnbInputDataSetDU")
    private String dnbInputDataSetDU;
    @JsonProperty("SeedInputDataSetDomain")
    private Map<String, String> seedInputDataSetDomain;
    @JsonProperty("SeedInputDataSetDuns")
    private Map<String, String> seedInputDataSetDuns;
    @JsonProperty("MiniInputDataSetType")
    private String miniInputDataSetType;
    @JsonProperty("MiniInputDataSetValue")
    private String miniInputDataSetValue;
    @JsonProperty("OutputDataSetType")
    private String outputDataSetType;
    @JsonProperty("OutputDataSetValue")
    private String outputDataSetValue;
    @JsonProperty("IterateStrategy")
    private IterativeStepConfig.ConvergeOnCount iterateStrategy;

    public String getDnbInputDataSetDomain() {
        return dnbInputDataSetDomain;
    }

    public void setDnbInputDataSetDomain(String dnbInputDataSetDomain) {
        this.dnbInputDataSetDomain = dnbInputDataSetDomain;
    }

    public String getDnbInputDataSetDuns() {
        return dnbInputDataSetDuns;
    }

    public void setDnbInputDataSetDuns(String dnbInputDataSetDuns) {
        this.dnbInputDataSetDuns = dnbInputDataSetDuns;
    }

    public String getDnbInputDataSetGU() {
        return dnbInputDataSetGU;
    }

    public void setDnbInputDataSetGU(String dnbInputDataSetGU) {
        this.dnbInputDataSetGU = dnbInputDataSetGU;
    }

    public String getDnbInputDataSetDU() {
        return dnbInputDataSetDU;
    }

    public void setDnbInputDataSetDU(String dnbInputDataSetDU) {
        this.dnbInputDataSetDU = dnbInputDataSetDU;
    }

    public Map<String, String> getSeedInputDataSetDomain() {
        return seedInputDataSetDomain;
    }

    public void setSeedInputDataSetDomain(Map<String, String> seedInputDataSetDomain) {
        this.seedInputDataSetDomain = seedInputDataSetDomain;
    }

    public Map<String, String> getSeedInputDataSetDuns() {
        return seedInputDataSetDuns;
    }

    public void setSeedInputDataSetDuns(Map<String, String> seedInputDataSetDuns) {
        this.seedInputDataSetDuns = seedInputDataSetDuns;
    }

    public String getMiniInputDataSetType() {
        return miniInputDataSetType;
    }

    public void setMiniInputDataSetType(String miniInputDataSetType) {
        this.miniInputDataSetType = miniInputDataSetType;
    }

    public String getMiniInputDataSetValue() {
        return miniInputDataSetValue;
    }

    public void setMiniInputDataSetValue(String miniInputDataSetValue) {
        this.miniInputDataSetValue = miniInputDataSetValue;
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

    public IterativeStepConfig.ConvergeOnCount getIterateStrategy() {
        return iterateStrategy;
    }

    public void setIterateStrategy(IterativeStepConfig.ConvergeOnCount iterateStrategy) {
        this.iterateStrategy = iterateStrategy;
    }
}
