package com.latticeengines.domain.exposed.datacloud.customer;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IncorrectMatchedAtttributeReproduceDetail extends ReproduceDetail{

    @JsonProperty("Attribute")
    private String attribute;

    @JsonProperty("MatchedValue")
    private String matchedValue;

    @Override
    @JsonProperty("ReproduceDetailType")
    public String getReproduceDetailType() {
        return this.getClass().getSimpleName();
    }

    public String getAttribute() {
        return attribute;
    }
    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public String getMatchedValue() {
        return matchedValue;
    }
    public void setMatchedValue(String matchedValue) {
        this.matchedValue = matchedValue;
    }

}
