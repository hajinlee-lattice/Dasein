package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AMAttrEnrichConfig extends TransformerConfig {
    @JsonProperty("NotJoinAM")
    private boolean notJoinAM;

    @JsonProperty("InputLatticeId")
    private String inputLatticeId = "LatticeAccountId";

    @JsonProperty("AMLatticeId")
    private String amLatticeId = "LatticeID";

    public boolean isNotJoinAM() {
        return notJoinAM;
    }

    public void setNotJoinAM(boolean notJoinAM) {
        this.notJoinAM = notJoinAM;
    }

    public String getInputLatticeId() {
        return inputLatticeId;
    }

    public void setInputLatticeId(String inputLatticeId) {
        this.inputLatticeId = inputLatticeId;
    }

    public String getAmLatticeId() {
        return amLatticeId;
    }

    public void setAmLatticeId(String amLatticeId) {
        this.amLatticeId = amLatticeId;
    }
}
