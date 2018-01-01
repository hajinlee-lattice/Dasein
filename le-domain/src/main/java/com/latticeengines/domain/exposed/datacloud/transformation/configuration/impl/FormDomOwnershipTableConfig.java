package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FormDomOwnershipTableConfig extends TransformerConfig {
    @JsonProperty("OrbSecPriDomain")
    private String orbSecPriDom;

    @JsonProperty("OrbSrcSecDomain")
    private String orbSrcSecDom;

    @JsonProperty("AmSeedDomain")
    private String amSeedDomain;

    @JsonProperty("AmSeedDuns")
    private String amSeedDuns;

    @JsonProperty("AmSeedDuDuns")
    private String amSeedDuDuns;

    @JsonProperty("AmSeedGuDuns")
    private String amSeedGuDuns;

    @JsonProperty("UsSalesVolume")
    private String usSalesVolume;

    @JsonProperty("TotalEmployees")
    private String totalEmployees;

    @JsonProperty("NumOfLocations")
    private String numOfLocations;

    @JsonProperty("FranchiseThreshold")
    private int franchiseThreshold;

    @JsonProperty("MultLargeCompThreshold")
    private Long multLargeCompThreshold;

    public String getAmSeedDuns() {
        return amSeedDuns;
    }

    public void setAmSeedDuns(String amSeedDuns) {
        this.amSeedDuns = amSeedDuns;
    }

    public String getAmSeedDuDuns() {
        return amSeedDuDuns;
    }

    public void setAmSeedDuDuns(String amSeedDuDuns) {
        this.amSeedDuDuns = amSeedDuDuns;
    }

    public String getAmSeedGuDuns() {
        return amSeedGuDuns;
    }

    public void setAmSeedGuDuns(String amSeedGuDuns) {
        this.amSeedGuDuns = amSeedGuDuns;
    }

    public String getUsSalesVolume() {
        return usSalesVolume;
    }

    public void setUsSalesVolume(String usSalesVolume) {
        this.usSalesVolume = usSalesVolume;
    }

    public String getTotalEmp() {
        return totalEmployees;
    }

    public void setTotalEmp(String totalEmployees) {
        this.totalEmployees = totalEmployees;
    }

    public String getNumOfLoc() {
        return numOfLocations;
    }

    public void setNumOfLoc(String numOfLocations) {
        this.numOfLocations = numOfLocations;
    }

    public String getAmSeedDomain() {
        return amSeedDomain;
    }

    public void setAmSeedDomain(String amSeedDomain) {
        this.amSeedDomain = amSeedDomain;
    }

    public String getOrbSecPriDom() {
        return orbSecPriDom;
    }

    public void setOrbSecPriDom(String orbSecPriDom) {
        this.orbSecPriDom = orbSecPriDom;
    }

    public String getOrbSrcSecDom() {
        return orbSrcSecDom;
    }

    public void setOrbSrcSecDom(String orbSrcSecDom) {
        this.orbSrcSecDom = orbSrcSecDom;
    }

    public int getFranchiseThreshold() {
        return franchiseThreshold;
    }

    public void setFranchiseThreshold(int franchiseThreshold) {
        this.franchiseThreshold = franchiseThreshold;
    }

    public Long getMultLargeCompThreshold() {
        return multLargeCompThreshold;
    }

    public void setMultLargeCompThreshold(Long multLargeCompThreshold) {
        this.multLargeCompThreshold = multLargeCompThreshold;
    }

}
