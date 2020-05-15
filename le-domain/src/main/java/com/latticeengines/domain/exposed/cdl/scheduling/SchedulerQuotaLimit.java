package com.latticeengines.domain.exposed.cdl.scheduling;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SchedulerQuotaLimit {

    @JsonProperty("large_account_countlimit")
    private Long largeAccountCountLimit;

    @JsonProperty("large_transaction_countlimit")
    private Long largeTransactionCountLimit;

    @JsonProperty("retry_countlimit")
    private Integer retryCountLimit;

    @JsonProperty("schedulenow_countlimit")
    private Integer scheduleNowCountLimit;

    @JsonProperty("large_countlimit")
    private Integer largeCountLimit;

    @JsonProperty("total_count")
    private Integer totalCount;

    @JsonProperty("retry_expired_time")
    private Long retryExpiredTime;

    public Long getLargeAccountCountLimit() {
        return largeAccountCountLimit;
    }

    public void setLargeAccountCountLimit(Long largeAccountCountLimit) {
        this.largeAccountCountLimit = largeAccountCountLimit;
    }

    public Long getLargeTransactionCountLimit() {
        return largeTransactionCountLimit;
    }

    public void setLargeTransactionCountLimit(Long largeTransactionCountLimit) {
        this.largeTransactionCountLimit = largeTransactionCountLimit;
    }

    public Integer getRetryCountLimit() {
        return retryCountLimit;
    }

    public void setRetryCountLimit(Integer retryCountLimit) {
        this.retryCountLimit = retryCountLimit;
    }

    public Integer getScheduleNowCountLimit() {
        return scheduleNowCountLimit;
    }

    public void setScheduleNowCountLimit(Integer scheduleNowCountLimit) {
        this.scheduleNowCountLimit = scheduleNowCountLimit;
    }

    public Integer getLargeCountLimit() {
        return largeCountLimit;
    }

    public void setLargeCountLimit(Integer largeCountLimit) {
        this.largeCountLimit = largeCountLimit;
    }

    public Integer getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;
    }

    public Long getRetryExpiredTime() {
        return retryExpiredTime;
    }

    public void setRetryExpiredTime(Long retryExpiredTime) {
        this.retryExpiredTime = retryExpiredTime;
    }
}
