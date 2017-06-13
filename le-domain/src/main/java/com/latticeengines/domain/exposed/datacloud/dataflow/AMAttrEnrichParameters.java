package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AMAttrEnrichParameters extends TransformationFlowParameters {
    @JsonProperty("NotJoinAM")
    private boolean notJoinAM;

    @JsonProperty("InputLatticeId")
    private String inputLatticeId = "LatticeAccountId";

    @JsonProperty("AMLatticeId")
    private String amLatticeId = "LatticeID";

    @JsonProperty("InputAttrs")
    private List<String> inputAttrs;

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

    public List<String> getInputAttrs() {
        return inputAttrs;
    }

    public void setInputAttrs(List<String> inputAttrs) {
        this.inputAttrs = inputAttrs;
    }

}
