package com.latticeengines.domain.exposed.cdl.scheduling;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PAQuotaSummary {

    @JsonProperty("is_hand_hold_pa_tenant")
    private boolean isHandHoldPATenant;

    @JsonProperty("message")
    private String message;

    @JsonProperty("remaining_pa_quota")
    private Map<String, Long> remainingPaQuota;

    @JsonProperty("timezone")
    private String timezone;

    @JsonProperty("quota_reset_time")
    private QuotaResetTime quotaResetTime;

    @JsonProperty("recently_completed_pas")
    private List<PASummary> recentlyCompletedPAs;

    @JsonIgnore
    public boolean isHandHoldPATenant() {
        return isHandHoldPATenant;
    }

    public void setHandHoldPATenant(boolean handHoldPATenant) {
        isHandHoldPATenant = handHoldPATenant;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Map<String, Long> getRemainingPaQuota() {
        return remainingPaQuota;
    }

    public void setRemainingPaQuota(Map<String, Long> remainingPaQuota) {
        this.remainingPaQuota = remainingPaQuota;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public QuotaResetTime getQuotaResetTime() {
        return quotaResetTime;
    }

    public void setQuotaResetTime(QuotaResetTime quotaResetTime) {
        this.quotaResetTime = quotaResetTime;
    }

    public List<PASummary> getRecentlyCompletedPAs() {
        return recentlyCompletedPAs;
    }

    public void setRecentlyCompletedPAs(List<PASummary> recentlyCompletedPAs) {
        this.recentlyCompletedPAs = recentlyCompletedPAs;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class PASummary {

        @JsonProperty("root_pa_started_at")
        private Instant rootJobStartedAt;

        @JsonProperty("completed_at")
        private Instant completedAt;

        @JsonProperty("application_id")
        private String applicationId;

        @JsonProperty("consumed_quota_name")
        private String consumedQuotaName;

        @JsonProperty("scheduled_queue_name")
        private String scheduledQueueName;

        public Instant getRootJobStartedAt() {
            return rootJobStartedAt;
        }

        public void setRootJobStartedAt(Instant rootJobStartedAt) {
            this.rootJobStartedAt = rootJobStartedAt;
        }

        public Instant getCompletedAt() {
            return completedAt;
        }

        public void setCompletedAt(Instant completedAt) {
            this.completedAt = completedAt;
        }

        public String getApplicationId() {
            return applicationId;
        }

        public void setApplicationId(String applicationId) {
            this.applicationId = applicationId;
        }

        public String getConsumedQuotaName() {
            return consumedQuotaName;
        }

        public void setConsumedQuotaName(String consumedQuotaName) {
            this.consumedQuotaName = consumedQuotaName;
        }

        public String getScheduledQueueName() {
            return scheduledQueueName;
        }

        public void setScheduledQueueName(String scheduledQueueName) {
            this.scheduledQueueName = scheduledQueueName;
        }
    }

    /*-
     * info about when PA quota of this tenant will be reset
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class QuotaResetTime {

        @JsonProperty("remaining_duration")
        private String remainingDuration;

        @JsonProperty("reset_at")
        private Instant resetAt;

        public String getRemainingDuration() {
            return remainingDuration;
        }

        public void setRemainingDuration(String remainingDuration) {
            this.remainingDuration = remainingDuration;
        }

        public Instant getResetAt() {
            return resetAt;
        }

        public void setResetAt(Instant resetAt) {
            this.resetAt = resetAt;
        }
    }

}
