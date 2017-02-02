package com.latticeengines.domain.exposed.datacloud.match;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LookupUpdateRequest {

    @JsonProperty("MatchKeys")
    private MatchKeyTuple matchKeys;

    @JsonProperty("LatticeAccountId")
    private String latticeAccountId;

    @JsonIgnore
    private String targetDuns;

    public MatchKeyTuple getMatchKeys() {
        return matchKeys;
    }

    public void setMatchKeys(MatchKeyTuple matchKeys) {
        this.matchKeys = matchKeys;
    }

    public String getLatticeAccountId() {
        return latticeAccountId;
    }

    public void setLatticeAccountId(String latticeAccountId) {
        this.latticeAccountId = latticeAccountId;
    }

    public String getTargetDuns() {
        return targetDuns;
    }

    public void setTargetDuns(String targetDuns) {
        this.targetDuns = targetDuns;
    }
}
