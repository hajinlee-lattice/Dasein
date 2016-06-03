package com.latticeengines.domain.exposed.modeling.review;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class BaseRuleResult {

    private int invalidItemCount;

    private DataRuleName dataRuleName;

    @JsonProperty
    public int getInvalidItemCount() {
        return invalidItemCount;
    }

    @JsonProperty
    public void setInvalidItemCount(int invalidItemCount) {
        this.invalidItemCount = invalidItemCount;
    }

    @JsonProperty
    public DataRuleName getDataRuleName() {
        return dataRuleName;
    }

    @JsonProperty
    public void setDataRuleName(DataRuleName dataRuleName) {
        this.dataRuleName = dataRuleName;
    }

}
