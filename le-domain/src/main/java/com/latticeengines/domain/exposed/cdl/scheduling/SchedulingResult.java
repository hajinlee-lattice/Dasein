package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/*
 * result of a single scheduling cycle
 */
public class SchedulingResult {
    private final Set<String> newPATenants;
    private final Set<String> retryPATenants;
    private final Map<String, Detail> details; // tenantId -> detail
    private final Set<String> allTenantsInQ; // tenants' Ids

    public SchedulingResult(Set<String> newPATenants, Set<String> retryPATenants, Map<String, Detail> details, Set<String> allTenantsInQ) {
        this.newPATenants = newPATenants == null ? Collections.emptySet() : newPATenants;
        this.retryPATenants = retryPATenants == null ? Collections.emptySet() : retryPATenants;
        this.details = details == null ? Collections.emptyMap() : details;
        this.allTenantsInQ = allTenantsInQ;
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

    /*
     * detail of scheduling result for a specific tenant
     */
    public static class Detail {
        private final String scheduledQueue;
        private final long waitTime;
        private final TenantActivity tenantActivity;

        public Detail(String scheduledQueue, long waitTime, TenantActivity tenantActivity) {
            this.scheduledQueue = scheduledQueue;
            this.waitTime = waitTime;
            this.tenantActivity = tenantActivity;
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

        @Override
        public String toString() {
            return "Detail{" + "scheduledQueue='" + scheduledQueue + '\'' + ", waitTime=" + waitTime
                    + ", tenantActivity=" + tenantActivity + '}';
        }
    }

    @Override
    public String toString() {
        return "SchedulingResult{" + "newPATenants=" + newPATenants + ", retryPATenants=" + retryPATenants
                + ", details=" + details + '}';
    }
}
