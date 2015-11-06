package com.latticeengines.domain.exposed.workflow;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "WORKFLOW_YARN_APP_ID")
public class YarnAppWorkflowId implements HasPid {

    private Long pid;
    private String yarnAppId;
    private Long workflowId;

    public YarnAppWorkflowId() {
        // default constructor
    }

    public YarnAppWorkflowId(ApplicationId yarnAppId, WorkflowId workflowId) {
        this.yarnAppId = yarnAppId.toString();
        this.workflowId = workflowId.getId();
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return this.pid;
    }

    @JsonIgnore
    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @JsonProperty("YarnAppId")
    @Column(name = "YARN_APP_ID", nullable = false)
    public String getYarnAppId() {
        return yarnAppId;
    }

    @JsonProperty("YarnAppId")
    public void setYarnAppId(String yarnAppId) {
        this.yarnAppId = yarnAppId;
    }

    @JsonProperty("WorkflowId")
    @Column(name = "WORKFLOW_ID", nullable = false)
    public Long getWorkflowId() {
        return workflowId;
    }

    @JsonProperty("WorkflowId")
    public void setWorkflowId(Long workflowId) {
        this.workflowId = workflowId;
    }

    @JsonIgnore
    @Transient
    public WorkflowId getAsWorkflowId() {
        return new WorkflowId(workflowId);
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((yarnAppId == null) ? 0 : yarnAppId.hashCode());
        result = prime * result + ((workflowId == null) ? 0 : workflowId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        YarnAppWorkflowId other = (YarnAppWorkflowId) obj;
        if (yarnAppId == null) {
            if (other.yarnAppId != null)
                return false;
        } else if (!yarnAppId.equals(other.yarnAppId))
            return false;
        if (workflowId == null) {
            if (other.workflowId != null)
                return false;
        } else if (!workflowId.equals(other.workflowId))
            return false;
        return true;
    }

}
