package com.latticeengines.yarn.exposed.runtime.metric;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;

public class InProgressJobMetric implements Dimension, Fact {

    private String applicationId;
    private String tenantId;
    private Boolean isWorkflowDriver;
    private Boolean isRunning;
    private Boolean isWaiting;
    private Integer elapsedTimeSec;
    private Integer allocatedMB;
    private Integer allocatedVCores;
    private Integer memorySeconds;
    private Integer runningContainers;
    private Integer vcoreSeconds;
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

    @MetricTag(tag = "IsWorkflowDriver")
    public String isWorkflowDriver() {
        return String.valueOf(isWorkflowDriver);
    }

    public void setIsWorkflowDriver(Boolean isWorkflowDriver) {
        this.isWorkflowDriver = isWorkflowDriver;
    }

    @MetricTag(tag = "IsRunning")
    public String isRunning() {
        return String.valueOf(isRunning);
    }

    public void setIsRunning(Boolean isRunning) {
        this.isRunning = isRunning;
    }

    @MetricTag(tag = "IsWaiting")
    public String isWaiting() {
        return String.valueOf(isWaiting);
    }

    public void setIsWaiting(Boolean isWaiting) {
        this.isWaiting = isWaiting;
    }

    @MetricField(name = "ElapsedTimeSec", fieldType = MetricField.FieldType.INTEGER)
    public Integer getElapsedTimeSec() {
        return elapsedTimeSec;
    }

    public void setElapsedTimeSec(Integer elapsedTimeSec) {
        this.elapsedTimeSec = elapsedTimeSec;
    }

    @MetricField(name = "AllocatedMB", fieldType = MetricField.FieldType.INTEGER)
    public Integer getAllocatedMB() {
        return allocatedMB;
    }

    public void setAllocatedMB(Integer allocatedMB) {
        this.allocatedMB = allocatedMB;
    }

    @MetricField(name = "AllocatedVCores", fieldType = MetricField.FieldType.INTEGER)
    public Integer getAllocatedVCores() {
        return allocatedVCores;
    }

    public void setAllocatedVCores(Integer allocatedVCores) {
        this.allocatedVCores = allocatedVCores;
    }

    @MetricField(name = "MemorySec", fieldType = MetricField.FieldType.INTEGER)
    public Integer getMemorySeconds() {
        return memorySeconds;
    }

    public void setMemorySeconds(Integer memorySeconds) {
        this.memorySeconds = memorySeconds;
    }

    @MetricField(name = "RunningContainers", fieldType = MetricField.FieldType.INTEGER)
    public Integer getRunningContainers() {
        return runningContainers;
    }

    public void setRunningContainers(Integer runningContainers) {
        this.runningContainers = runningContainers;
    }

    @MetricField(name = "VcoreSec", fieldType = MetricField.FieldType.INTEGER)
    public Integer getVcoreSeconds() {
        return vcoreSeconds;
    }

    public void setVcoreSeconds(Integer vcoreSeconds) {
        this.vcoreSeconds = vcoreSeconds;
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
