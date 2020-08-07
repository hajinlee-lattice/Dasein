package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.SetUtils;

/*
 * result of a single scheduling cycle
 */
public class SchedulingResult {
    private final Set<String> newPATenants;
    private final Set<String> retryPATenants;
    private final Map<String, Detail> details; // tenantId -> detail
    private final Set<String> allTenantsInQ; // tenants' Ids
    private final Map<String, List<ConstraintViolationReason>> constraintViolationReasons; // tenant id

    public SchedulingResult(Set<String> newPATenants, Set<String> retryPATenants, Map<String, Detail> details,
            Set<String> allTenantsInQ, Map<String, List<ConstraintViolationReason>> constraintViolationReasons) {
        this.newPATenants = SetUtils.emptyIfNull(newPATenants);
        this.retryPATenants = SetUtils.emptyIfNull(retryPATenants);
        this.details = MapUtils.emptyIfNull(details);
        this.allTenantsInQ = SetUtils.emptyIfNull(allTenantsInQ);
        this.constraintViolationReasons = MapUtils.emptyIfNull(constraintViolationReasons);
    }

    public Set<String> getNewPATenants() {
        return newPATenants;
    }

    public Set<String> getRetryPATenants() {
        return retryPATenants;
    }

    public Map<String, Detail> getDetails() {
        return details;
    }

    public Set<String> getAllTenantsInQ() {
        return allTenantsInQ;
    }

    public Map<String, List<ConstraintViolationReason>> getConstraintViolationReasons() {
        return constraintViolationReasons;
    }

    public static class ConstraintViolationReason {
        private final String queueName;
        private final String reason;

        public ConstraintViolationReason(String queueName, String reason) {
            this.queueName = queueName;
            this.reason = reason;
        }

        public String getQueueName() {
            return queueName;
        }

        public String getReason() {
            return reason;
        }

        @Override
        public String toString() {
            return "ConstraintViolationReason{" + "queueName='" + queueName + '\'' + ", reason='" + reason + '\'' + '}';
        }
    }

    /*
     * detail of scheduling result for a specific tenant
     */
    public static class Detail {
        private final String scheduledQueue;
        private final long waitTime;
        private final TenantActivity tenantActivity;
        private final String consumedQuotaName;

        public Detail(String scheduledQueue, long waitTime, TenantActivity tenantActivity, String consumedQuotaName) {
            this.scheduledQueue = scheduledQueue;
            this.waitTime = waitTime;
            this.tenantActivity = tenantActivity;
            this.consumedQuotaName = consumedQuotaName;
        }

        public String getScheduledQueue() {
            return scheduledQueue;
        }

        public long getWaitTime() {
            return waitTime;
        }

        public TenantActivity getTenantActivity() {
            return tenantActivity;
        }

        public String getConsumedQuotaName() {
            return consumedQuotaName;
        }

        @Override
        public String toString() {
            return "Detail{" + "scheduledQueue='" + scheduledQueue + '\'' + ", waitTime=" + waitTime
                    + ", tenantActivity=" + tenantActivity + ", quotaName='" + consumedQuotaName + '\'' + '}';
        }
    }

    @Override
    public String toString() {
        return "SchedulingResult{" + "newPATenants=" + newPATenants + ", retryPATenants=" + retryPATenants
                + ", details=" + details + '}';
    }
}
