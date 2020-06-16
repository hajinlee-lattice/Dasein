package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class UploadStats {

    @JsonProperty("import")
    private ImportStats importStats;

    @JsonProperty("match")
    private MatchStats matchStats;

    public ImportStats getImportStats() {
        return importStats;
    }

    public void setImportStats(ImportStats importStats) {
        this.importStats = importStats;
    }

    public MatchStats getMatchStats() {
        return matchStats;
    }

    public void setMatchStats(MatchStats matchStats) {
        this.matchStats = matchStats;
    }

    public static class ImportStats {

        @JsonProperty("success_cnt")
        private Long successCnt;

        @JsonProperty("error_cnt")
        private Long errorCnt;

        public Long getSuccessCnt() {
            return successCnt;
        }

        public void setSuccessCnt(Long successCnt) {
            this.successCnt = successCnt;
        }

        public Long getErrorCnt() {
            return errorCnt;
        }

        public void setErrorCnt(Long errorCnt) {
            this.errorCnt = errorCnt;
        }
    }

    public static class MatchStats {

        @JsonProperty("accepted_cnt")
        private Long acceptedCnt;

        @JsonProperty("pending_review_cnt")
        private Long pendingReviewCnt;

        @JsonProperty("rejected_cnt")
        private Long rejectedCnt;

        public Long getAcceptedCnt() {
            return acceptedCnt;
        }

        public void setAcceptedCnt(Long acceptedCnt) {
            this.acceptedCnt = acceptedCnt;
        }

        public Long getPendingReviewCnt() {
            return pendingReviewCnt;
        }

        public void setPendingReviewCnt(Long pendingReviewCnt) {
            this.pendingReviewCnt = pendingReviewCnt;
        }

        public Long getRejectedCnt() {
            return rejectedCnt;
        }

        public void setRejectedCnt(Long rejectedCnt) {
            this.rejectedCnt = rejectedCnt;
        }
    }

}
