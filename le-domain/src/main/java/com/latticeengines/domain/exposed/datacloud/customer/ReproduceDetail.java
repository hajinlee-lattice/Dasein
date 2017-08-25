package com.latticeengines.domain.exposed.datacloud.customer;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "ReproduceDetailType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = IncorreceLookupReproduceDetail.class, name = "IncorreceLookupReproduceDetail"),
        @JsonSubTypes.Type(value = IncorrectMatchedAtttributeReproduceDetail.class, name = "IncorrectMatchedAtttributeReproduceDetail"),
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class ReproduceDetail {

    @JsonProperty("InputKeys")
    Map<MatchKey, Object> inputKeys;

    @JsonProperty("MatchedKeys")
    Map<MatchKey, Object> matchedKeys;

    public Map<MatchKey, Object> getInputKeys() {
        return inputKeys;
    }

    public void setInputKeys(Map<MatchKey, Object> inputKeys) {
        this.inputKeys = inputKeys;
    }

    public Map<MatchKey, Object> getMatchedKeys() {
        return matchedKeys;
    }

    public void setMatchedKeys(Map<MatchKey, Object> matchedKeys) {
        this.matchedKeys = matchedKeys;
    }

    @JsonIgnore
    protected abstract String getReproduceDetailType();

}
