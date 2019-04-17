package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AMSeedDeriveAttrsConfig extends TransformerConfig {

    @JsonProperty("AmSeedDuns")
    private String amSeedDuns;

    @JsonProperty("AmSeedDuDuns")
    private String amSeedDuDuns;

    @JsonProperty("AmSeedGuDuns")
    private String amSeedGuDuns;

    @JsonProperty("AmSeedParentDuns")
    private String amSeedParentDuns;

    @JsonProperty("UsSalesVolume")
    private String usSalesVolume;

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

    public String getAmSeedParentDuns() {
        return amSeedParentDuns;
    }

    public void setAmSeedParentDuns(String amSeedParentDuns) {
        this.amSeedParentDuns = amSeedParentDuns;
    }

    public String getUsSalesVolume() {
        return usSalesVolume;
    }

    public void setUsSalesVolume(String usSalesVolume) {
        this.usSalesVolume = usSalesVolume;
    }

}
