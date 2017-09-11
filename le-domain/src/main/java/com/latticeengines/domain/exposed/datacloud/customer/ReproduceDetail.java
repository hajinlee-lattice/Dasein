package com.latticeengines.domain.exposed.datacloud.customer;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "ReproduceDetailType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = IncorrectLookupReproduceDetail.class, name = "IncorrectLookupReproduceDetail"),
        @JsonSubTypes.Type(value = IncorrectMatchedAttributeReproduceDetail.class, name = "IncorrectMatchedAttributeReproduceDetail"),
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class ReproduceDetail {

    @JsonProperty("InputKeys")
    Map<String, String> inputKeys;

    @JsonProperty("MatchedKeys")
    Map<String, String> matchedKeys;

    public Map<String, String> getInputKeys() {
        return inputKeys;
    }

    public void setInputKeys(Map<String, String> inputKeys) {
        this.inputKeys = inputKeys;
    }

    public Map<String, String> getMatchedKeys() {
        return matchedKeys;
    }

    public void setMatchedKeys(Map<String, String> matchedKeys) {
        Map<String, String> editedKeys = new HashMap<String, String>();
        for (Map.Entry<String, String> entry : matchedKeys.entrySet()) {
            String key = entry.getKey();
            String resultKey = findKeyInMatchKey(key);
            editedKeys.put(resultKey, entry.getValue());
        }
        this.matchedKeys = editedKeys;
    }

    private String findKeyInMatchKey(String key) {
        MatchKey[] keys = MatchKey.values();
        MatchKey found = null;
        for (MatchKey entry : keys) {
            if (key.toLowerCase().contains(entry.name().toLowerCase())) {
                found = entry;
                break;
            }
        }
        if (found != null) {
            return found.name();
        }
        return key;
    }

    @JsonIgnore
    protected abstract String getReproduceDetailType();

}
