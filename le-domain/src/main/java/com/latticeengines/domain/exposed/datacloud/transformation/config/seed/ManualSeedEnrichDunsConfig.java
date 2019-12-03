package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ManualSeedEnrichDunsConfig extends TransformerConfig {

    @JsonProperty("MAN_Id")
    private String manId;

    @JsonProperty("MAN_DUNS")
    private String manDuns;

    public String getManSeedId() {
        return manId;
    }

    public void setManSeedId(String manId) {
        this.manId = manId;
    }

    public String getManSeedDuns() {
        return manDuns;
    }

    public void setManSeedDuns(String manDuns) {
        this.manDuns = manDuns;
    }

}
