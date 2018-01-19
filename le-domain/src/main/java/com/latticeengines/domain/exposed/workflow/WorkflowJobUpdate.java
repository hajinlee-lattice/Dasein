package com.latticeengines.domain.exposed.workflow;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.hibernate.annotations.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.*;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "WORKFLOW_JOB_UPDATE",
       indexes = @Index(name = "IX_WORKFLOW_PID", columnList = "WORKFLOW_PID"))
public class WorkflowJobUpdate implements HasPid {
    private static final Logger log = LoggerFactory.getLogger(WorkflowJobUpdate.class);

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "WORKFLOW_PID", nullable = false)
    private Long workflowPid;

    @Column(name = "LAST_UPDATE_TIME", nullable = false)
    private Long lastUpdateTime;

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

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }
}
