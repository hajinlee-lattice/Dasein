package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class MatchKeyFill {

    @JsonProperty("display_name")
    private String displayName;

    @JsonProperty("match_key")
    private MatchKey matchKey;

    @JsonProperty("populated")
    private long populated;

    @JsonProperty("missing")
    private long missing;

    @JsonProperty("ingested")
    private long ingested;

    @JsonProperty("fill_rate")
    private int fillRate;

    public MatchKey getMatchKey() {
        return matchKey;
    }

    public void setMatchKey(MatchKey matchKey) {
        this.matchKey = matchKey;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public long getPopulated() {
        return populated;
    }

    public void setPopulated(long populated) {
        this.populated = populated;
    }

    public long getMissing() {
        return missing;
    }

    public void setMissing(long missing) {
        this.missing = missing;
    }

    public long getIngested() {
        return ingested;
    }

    public void setIngested(long ingested) {
        this.ingested = ingested;
    }

    public int getFillRate() {
        return fillRate;
    }

    public void setFillRate(int fillRate) {
        this.fillRate = fillRate;
    }
}
