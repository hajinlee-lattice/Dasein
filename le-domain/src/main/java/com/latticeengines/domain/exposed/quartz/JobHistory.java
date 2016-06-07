package com.latticeengines.domain.exposed.quartz;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "JOB_HISTORY")
public class JobHistory implements HasPid, Serializable {

    private static final long serialVersionUID = -2804059029306935710L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @JsonIgnore
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("job_name")
    @Column(name = "JobName", nullable = false)
    private String jobName;

    @JsonProperty("tenant_id")
    @Column(name = "TenantId", nullable = false)
    private String tenantId;

    @JsonProperty("triggered_job_handle")
    @Column(name = "TriggeredJobHandle", nullable = true)
    private String triggeredJobHandle;

    @JsonProperty("triggered_time")
    @Column(name = "TriggeredTime", nullable = false)
    private Date triggeredTime;

    @JsonProperty("triggered_job_status")
    @Column(name = "TriggeredJobStatus", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    private TriggeredJobStatus triggeredJobStatus;

    @JsonProperty("error_message")
    @Column(name = "ErrorMessage", nullable = true)
    private String errorMessage;

    @JsonProperty("execution_host")
    @Column(name = "ExecutionHost", nullable = true)
    private String executionHost;

    @Override
    public Long getPid() {
        return this.pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getTriggeredJobHandle() {
        return triggeredJobHandle;
    }

    public void setTriggeredJobHandle(String triggeredJobHandle) {
        this.triggeredJobHandle = triggeredJobHandle;
    }

    public Date getTriggeredTime() {
        return triggeredTime;
    }

    public void setTriggeredTime(Date triggeredTime) {
        this.triggeredTime = triggeredTime;
    }

    public TriggeredJobStatus getTriggeredJobStatus() {
        return triggeredJobStatus;
    }

    public void setTriggeredJobStatus(TriggeredJobStatus triggeredJobStatus) {
        this.triggeredJobStatus = triggeredJobStatus;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getExecutionHost() {
        return executionHost;
    }

    public void setExecutionHost(String executionHost) {
        this.executionHost = executionHost;
    }
}
