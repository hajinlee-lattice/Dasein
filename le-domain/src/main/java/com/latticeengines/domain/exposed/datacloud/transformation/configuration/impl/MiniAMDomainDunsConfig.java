package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MiniAMDomainDunsConfig extends TransformerConfig {
    @JsonProperty("DnbInputDataSetDomain")
    private String dnbInputDataSetDomain;
    @JsonProperty("DnbInputDataSetDuns")
    private String dnbInputDataSetDuns;
    @JsonProperty("DnbInputDataSetGU")
    private String dnbInputDataSetGU;
    @JsonProperty("DnbInputDataSetDU")
    private String dnbInputDataSetDU;
    @JsonProperty("AmInputDataSetDomain")
    private String amInputDataSetDomain;
    @JsonProperty("AmInputDataSetDuns")
    private String amInputDataSetDuns;
    @JsonProperty("MiniInputDataSetDomain")
    private String miniInputDataSetDomain;
    @JsonProperty("MiniInputDataSetDuns")
    private String miniInputDataSetDuns;
    @JsonProperty("OutputDataSetType")
    private String outputDataSetType;
    @JsonProperty("OutputDataSetValue")
    private String outputDataSetValue;

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

    public String getAmInputDataSetDomain() {
        return amInputDataSetDomain;
    }

    public void setAmInputDataSetDomain(String amInputDataSetDomain) {
        this.amInputDataSetDomain = amInputDataSetDomain;
    }

    public String getAmInputDataSetDuns() {
        return amInputDataSetDuns;
    }

    public void setAmInputDataSetDuns(String amInputDataSetDuns) {
        this.amInputDataSetDuns = amInputDataSetDuns;
    }

    public String getMiniInputDataSetDomain() {
        return miniInputDataSetDomain;
    }

    public void setMiniInputDataSetDomain(String miniInputDataSetDomain) {
        this.miniInputDataSetDomain = miniInputDataSetDomain;
    }

    public String getMiniInputDataSetDuns() {
        return miniInputDataSetDuns;
    }

    public void setMiniInputDataSetDuns(String miniInputDataSetDuns) {
        this.miniInputDataSetDuns = miniInputDataSetDuns;
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
