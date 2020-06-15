package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class UploadStats {

    @JsonProperty("submitted")
    private Long submitted;

    @JsonProperty("successfullyIngested")
    private Long successfullyIngested;

    @JsonProperty("failedIngested")
    private Long failedIngested;

    @JsonProperty("matched")
    private Long matched;

    @JsonProperty("pendingReviewCnt")
    private Long pendingReviewCnt;

    @JsonProperty("unmatched")
    private Long unmatched;

    public Long getSubmitted() {
        return submitted;
    }

    public void setSubmitted(Long submitted) {
        this.submitted = submitted;
    }

    public Long getSuccessfullyIngested() {
        return successfullyIngested;
    }

    public void setSuccessfullyIngested(Long successfullyIngested) {
        this.successfullyIngested = successfullyIngested;
    }

    public Long getFailedIngested() {
        return failedIngested;
    }

    public void setFailedIngested(Long failedIngested) {
        this.failedIngested = failedIngested;
    }

    public Long getMatched() {
        return matched;
    }

    public void setMatched(Long matched) {
        this.matched = matched;
    }

    public Long getPendingReviewCnt() {
        return pendingReviewCnt;
    }

    public void setPendingReviewCnt(Long pendingReviewCnt) {
        this.pendingReviewCnt = pendingReviewCnt;
    }

    public Long getUnmatched() {
        return unmatched;
    }

    public void setUnmatched(Long unmatched) {
        this.unmatched = unmatched;
    }
}
