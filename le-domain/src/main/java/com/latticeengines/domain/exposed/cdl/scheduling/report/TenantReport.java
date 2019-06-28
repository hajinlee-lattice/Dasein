package com.latticeengines.domain.exposed.cdl.scheduling.report;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Preconditions;

/**
 * Scheduling information for single tenant
 */
public class TenantReport {
    private final String tenantId;

    private final long lastEvaluationTime;

    private final long lastScheduledTime;

    private final List<ScheduledPA> scheduledPAs;

    // Pair(cycle ID, list(scheduling decision for one queue))
    private final List<Pair<Integer, List<SchedulingDecision>>> schedulingDecisions;

    public TenantReport(String tenantId, long lastEvaluationTime, long lastScheduledTime,
            List<ScheduledPA> scheduledPAs, List<Pair<Integer, List<SchedulingDecision>>> schedulingDecisions) {
        Preconditions.checkArgument(StringUtils.isNotBlank(tenantId), "Tenant ID should not be blank");
        this.tenantId = tenantId;
        this.lastEvaluationTime = lastEvaluationTime;
        this.lastScheduledTime = lastScheduledTime;
        this.scheduledPAs = scheduledPAs != null ? scheduledPAs : Collections.emptyList();
        this.schedulingDecisions = schedulingDecisions != null ? schedulingDecisions : Collections.emptyList();
    }

    public String getTenantId() {
        return tenantId;
    }

    public long getLastEvaluationTime() {
        return lastEvaluationTime;
    }

    public long getLastScheduledTime() {
        return lastScheduledTime;
    }

    public List<ScheduledPA> getScheduledPAs() {
        return scheduledPAs;
    }

    public List<Pair<Integer, List<SchedulingDecision>>> getSchedulingDecisions() {
        return schedulingDecisions;
    }

    @Override
    public String toString() {
        return "TenantReport{" + "tenantId='" + tenantId + '\'' + ", lastEvaluationTime=" + lastEvaluationTime
                + ", lastScheduledTime=" + lastScheduledTime + ", scheduledPAs=" + scheduledPAs
                + ", schedulingDecisions=" + schedulingDecisions + '}';
    }

    /**
     * Scheduling information about a specific PA
     */
    public static class ScheduledPA {
        private final int cycleId;

        private final long scheduleTime;

        private final String applicationId;

        public ScheduledPA(int cycleId, long scheduleTime, String applicationId) {
            Preconditions.checkNotNull(applicationId, "ApplicationId should not be null");
            this.cycleId = cycleId;
            this.scheduleTime = scheduleTime;
            this.applicationId = applicationId;
        }

        public int getCycleId() {
            return cycleId;
        }

        public long getScheduleTime() {
            return scheduleTime;
        }

        public String getApplicationId() {
            return applicationId;
        }

        @Override
        public String toString() {
            return "ScheduledPA{" + "cycleId=" + cycleId + ", scheduleTime=" + scheduleTime + ", applicationId='"
                    + applicationId + '\'' + '}';
        }
    }

    /**
     * Scheduling decision made by one specific queue
     */
    public static class SchedulingDecision {
        private final String queueName;

        private final boolean isPAScheduled;

        private final String violatedConstraint;

        public SchedulingDecision(String queueName, boolean isPAScheduled, String violatedConstraint) {
            Preconditions.checkArgument(StringUtils.isNotBlank(queueName), "Queue name should not be blank");
            this.queueName = queueName;
            this.isPAScheduled = isPAScheduled;
            this.violatedConstraint = violatedConstraint != null ? violatedConstraint : "";
        }

        public String getQueueName() {
            return queueName;
        }

        public boolean isPAScheduled() {
            return isPAScheduled;
        }

        public String getViolatedConstraint() {
            return violatedConstraint;
        }

        @Override
        public String toString() {
            return "SchedulingDecision{" + "queueName='" + queueName + '\'' + ", isPAScheduled=" + isPAScheduled
                    + ", violatedConstraint='" + violatedConstraint + '\'' + '}';
        }
    }
}
