package com.latticeengines.domain.exposed.quartz;

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
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "QUARTZ_JOB_SOURCE")
public class JobSource implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @JsonIgnore
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonIgnore
    @Column(name = "JobName", nullable = false)
    private String jobName;

    @JsonIgnore
    @Column(name = "TenantId", nullable = false)
    private String tenantId;

    @JsonIgnore
    @Column(name = "JobSourceType", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    private JobSourceType sourceType;

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

    public JobSourceType getSourceType() {
        return sourceType;
    }

    public void setSourceType(JobSourceType sourceType) {
        this.sourceType = sourceType;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }
}
