package com.latticeengines.domain.exposed.cdl.scheduling.report;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

/**
 * Store scheduling related info for specific PA job
 */
public class PAJobReport {

    private final int cycleId;

    private final long scheduleTime;

    private final String applicationId;

    private final String tenantId;

    private final boolean isRetryPA;

    public PAJobReport(int cycleId, long scheduleTime, String applicationId, String tenantId, boolean isRetryPA) {
        Preconditions.checkArgument(StringUtils.isNotBlank(applicationId), "ApplicationId should not be blank");
        Preconditions.checkArgument(StringUtils.isNotBlank(tenantId), "Tenant ID should not be blank");
        this.cycleId = cycleId;
        this.scheduleTime = scheduleTime;
        this.applicationId = applicationId;
        this.tenantId = tenantId;
        this.isRetryPA = isRetryPA;
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

    public String getTenantId() {
        return tenantId;
    }

    public boolean isRetryPA() {
        return isRetryPA;
    }

    @Override
    public String toString() {
        return "PAJobReport{" + "cycleId=" + cycleId + ", scheduleTime=" + scheduleTime + ", tenantId='" + tenantId
                + '\'' + ", isRetryPA=" + isRetryPA + '}';
    }
}
