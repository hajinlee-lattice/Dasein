package com.latticeengines.domain.exposed.cdl.scheduling;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PASchedulerConfig {

    @JsonProperty("large_tenant_account_volume_threshold")
    private Long largeTenantAccountVolumeThreshold;

    @JsonProperty("large_tenant_txn_volume_threshold")
    private Long largeTenantTxnVolumeThreshold;

    @JsonProperty("concurrent_schedulenow_job_limit")
    private Integer concurrentScheduleNowJobLimit;

    @JsonProperty("concurrent_large_job_limit")
    private Integer concurrentLargeJobLimit;

    @JsonProperty("concurrent_large_txn_job_limit")
    private Integer concurrentLargeTxnJobLimit;

    @JsonProperty("concurrent_job_limit")
    private Integer concurrentJobLimit;

    @JsonProperty("retry_count_limit")
    private Integer retryCountLimit;

    @JsonProperty("retry_expired_time")
    private Long retryExpiredTime;

    public Long getLargeTenantAccountVolumeThreshold() {
        return largeTenantAccountVolumeThreshold;
    }

    public void setLargeTenantAccountVolumeThreshold(Long largeTenantAccountVolumeThreshold) {
        this.largeTenantAccountVolumeThreshold = largeTenantAccountVolumeThreshold;
    }

    public Long getLargeTenantTxnVolumeThreshold() {
        return largeTenantTxnVolumeThreshold;
    }

    public void setLargeTenantTxnVolumeThreshold(Long largeTenantTxnVolumeThreshold) {
        this.largeTenantTxnVolumeThreshold = largeTenantTxnVolumeThreshold;
    }

    public Integer getRetryCountLimit() {
        return retryCountLimit;
    }

    public void setRetryCountLimit(Integer retryCountLimit) {
        this.retryCountLimit = retryCountLimit;
    }

    public Integer getConcurrentScheduleNowJobLimit() {
        return concurrentScheduleNowJobLimit;
    }

    public void setConcurrentScheduleNowJobLimit(Integer concurrentScheduleNowJobLimit) {
        this.concurrentScheduleNowJobLimit = concurrentScheduleNowJobLimit;
    }

    public Integer getConcurrentLargeJobLimit() {
        return concurrentLargeJobLimit;
    }

    public void setConcurrentLargeJobLimit(Integer concurrentLargeJobLimit) {
        this.concurrentLargeJobLimit = concurrentLargeJobLimit;
    }

    public Integer getConcurrentLargeTxnJobLimit() {
        return concurrentLargeTxnJobLimit;
    }

    public void setConcurrentLargeTxnJobLimit(Integer concurrentLargeTxnJobLimit) {
        this.concurrentLargeTxnJobLimit = concurrentLargeTxnJobLimit;
    }

    public Integer getConcurrentJobLimit() {
        return concurrentJobLimit;
    }

    public void setConcurrentJobLimit(Integer concurrentJobLimit) {
        this.concurrentJobLimit = concurrentJobLimit;
    }

    public Long getRetryExpiredTime() {
        return retryExpiredTime;
    }

    public void setRetryExpiredTime(Long retryExpiredTime) {
        this.retryExpiredTime = retryExpiredTime;
    }

    @Override
    public String toString() {
        return "PASchedulerConfig{" + "largeTenantAccountVolumeThreshold=" + largeTenantAccountVolumeThreshold
                + ", largeTenantTxnVolumeThreshold=" + largeTenantTxnVolumeThreshold + ", retryCountLimit="
                + retryCountLimit + ", concurrentScheduleNowJobLimit=" + concurrentScheduleNowJobLimit
                + ", concurrentLargeJobLimit=" + concurrentLargeJobLimit + ", concurrentLargeTxnJobLimit="
                + concurrentLargeTxnJobLimit + ", concurrentJobLimit=" + concurrentJobLimit + ", retryExpiredTime="
                + retryExpiredTime + '}';
    }
}
