package com.latticeengines.domain.exposed.workflow;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "WORKFLOW_JOB_UPDATE", //
        indexes = @Index(name = "IX_WORKFLOW_PID", columnList = "WORKFLOW_PID"))
public class WorkflowJobUpdate implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "WORKFLOW_PID", nullable = false)
    private Long workflowPid;

    @Column(name = "LAST_UPDATE_TIME", nullable = false)
    private Long lastUpdateTime;

    @Column(name = "CREATE_TIME", nullable = false)
    private Long createTime;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public Long getWorkflowPid() {
        return workflowPid;
    }

    public void setWorkflowPid(Long workflowPid) {
        this.workflowPid = workflowPid;
    }

    public Long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public Long getCreateTime() {
        return this.createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() { return HashCodeBuilder.reflectionHashCode(this); }
}
