package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ManualSeedTransformerConfig extends TransformerConfig {

    // 0 position stored manual seed field and 1 position stores am seed field
    @JsonProperty("OverwriteFields")
    private String[][] overwriteFields;

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

    @JsonProperty("AmSeedLeIsPrimDom")
    private String amSeedLeIsPrimDom;

    @JsonProperty("IsPrimaryAccount")
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

    public String getIsPrimaryAccount() {
        return isPrimaryAccount;
    }

    public void setIsPrimaryAccount(String isPrimaryAccount) {
        this.isPrimaryAccount = isPrimaryAccount;
    }

    public String[][] getOverwriteFields() {
        return overwriteFields;
    }

    public void setOverwriteFieldsArray(String[][] overwriteFields) {
        this.overwriteFields = overwriteFields;
    }

}
