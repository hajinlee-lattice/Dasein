package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ManualSeedTransformerConfig extends TransformerConfig {

    @JsonProperty("FixTreeFlag")
    private String fixTreeFlag;

    @JsonProperty("AmSeedDuDuns")
    private String amSeedDuDuns;

    @JsonProperty("ManualSeedDuns")
    private String manualSeedDuns;

    @JsonProperty("AmSeedDuns")
    private String amSeedDuns;

    @JsonProperty("ManualSeedDomain")
    private String manualSeedDomain;

    @JsonProperty("AmSeedDomain")
    private String amSeedDomain;

    @JsonProperty("ManualSeedSalesVolume")
    private String manualSeedSalesVolume;

    @JsonProperty("ManualSeedTotalEmp")
    private String manualSeedTotalEmp;

    @JsonProperty("AmSeedSalesVolume")
    private String amSeedSalesVolume;

    @JsonProperty("AmSeedTotalEmp")
    private String amSeedTotalEmp;

    @JsonProperty("AmSeedLeIsPrimDom")
    private String amSeedLeIsPrimDom;

    @JsonProperty("AmSeedLdcCity")
    private String amSeedLdcCity;

    @JsonProperty("isPrimaryAccount")
    private String isPrimaryAccount;

    public String getFixTreeFlag() {
        return fixTreeFlag;
    }

    public void setFixTreeFlag(String fixTreeFlag) {
        this.fixTreeFlag = fixTreeFlag;
    }

    public String getAmSeedDuDuns() {
        return amSeedDuDuns;
    }

    public void setAmSeedDuDuns(String amSeedDuDuns) {
        this.amSeedDuDuns = amSeedDuDuns;
    }

    public String getManualSeedDuns() {
        return manualSeedDuns;
    }

    public void setManualSeedDuns(String manualSeedDuns) {
        this.manualSeedDuns = manualSeedDuns;
    }

    public String getManualSeedTotalEmp() {
        return manualSeedTotalEmp;
    }

    public void setManualSeedTotalEmp(String manualSeedTotalEmp) {
        this.manualSeedTotalEmp = manualSeedTotalEmp;
    }

    public String getAmSeedTotalEmp() {
        return amSeedTotalEmp;
    }

    public void setAmSeedTotalEmp(String amSeedTotalEmp) {
        this.amSeedTotalEmp = amSeedTotalEmp;
    }

    public String getManualSeedSalesVolume() {
        return manualSeedSalesVolume;
    }

    public void setManualSeedSalesVolume(String manualSeedSalesVolume) {
        this.manualSeedSalesVolume = manualSeedSalesVolume;
    }

    public String getAmSeedSalesVolume() {
        return amSeedSalesVolume;
    }

    public void setAmSeedSalesVolume(String amSeedSalesVolume) {
        this.amSeedSalesVolume = amSeedSalesVolume;
    }

    public String getAmSeedDuns() {
        return amSeedDuns;
    }

    public void setAmSeedDuns(String amSeedDuns) {
        this.amSeedDuns = amSeedDuns;
    }

    public String getManualSeedDomain() {
        return manualSeedDomain;
    }

    public void setManualSeedDomain(String manualSeedDomain) {
        this.manualSeedDomain = manualSeedDomain;
    }

    public String getAmSeedDomain() {
        return amSeedDomain;
    }

    public void setAmSeedDomain(String amSeedDomain) {
        this.amSeedDomain = amSeedDomain;
    }

    public String getAmSeedLeIsPrimDom() {
        return amSeedLeIsPrimDom;
    }

    public void setAmSeedLeIsPrimDom(String amSeedLeIsPrimDom) {
        this.amSeedLeIsPrimDom = amSeedLeIsPrimDom;
    }

    public String getAmSeedLdcCity() {
        return amSeedLdcCity;
    }

    public void setAmSeedLdcCity(String amSeedLdcCity) {
        this.amSeedLdcCity = amSeedLdcCity;
    }

    public String getIsPrimaryAccount() {
        return isPrimaryAccount;
    }

    public void setIsPrimaryAccount(String isPrimaryAccount) {
        this.isPrimaryAccount = isPrimaryAccount;
    }

}
