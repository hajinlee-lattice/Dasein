package com.latticeengines.yarn.exposed.runtime.metric;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;

public class CompletedJobMetric implements Dimension, Fact {

    private String applicationId;
    private String tenantId;
    private String jobName;
    private String queue;
    private String finalAppStatus;

    @MetricField(name = "ApplicationId", fieldType = MetricField.FieldType.STRING)
    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    @MetricTag(tag = "TenantId")
    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    @MetricTag(tag = "JobName")
    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    @MetricTag(tag = "Queue")
    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    @MetricTag(tag = "FinalAppStatus")
    public String getFinalAppStatus() {
        return finalAppStatus;
    }

    public void setFinalAppStatus(String finalAppStatus) {
        this.finalAppStatus = finalAppStatus;
    }
}
