package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AudienceEventDetail extends EventDetail {

    public AudienceEventDetail() {
        super("Audience");
    }

    @JsonProperty("audience_size")
    private Long audienceSize;

    @JsonProperty("matched_count")
    private Long matchedCount;

    @JsonProperty("matched_rate")
    private Long matchRate;

    private String audienceId;

    private String audienceName;

    public String getAudienceId() {
        return audienceId;
    }

    public void setAudienceId(String audienceId) {
        this.audienceId = audienceId;
    }

    public String getAudienceName() {
        return audienceName;
    }

    public void setAudienceName(String audienceName) {
        this.audienceName = audienceName;
    }

    public Long getAudienceSize() {
        return audienceSize;
    }

    public void setAudienceSize(Long audienceSize) {
        this.audienceSize = audienceSize;
    }

    public Long getMatchedCount() {
        return matchedCount;
    }

    public void setMatchedCount(Long matchedCount) {
        this.matchedCount = matchedCount;
    }

    public void setMatchRate(Long matchRate) {
        this.matchRate = matchRate;
    }

    public Long getMatchRate() {
        return matchRate;
    }

}
