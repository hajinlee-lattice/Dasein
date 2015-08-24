package com.latticeengines.domain.exposed.dataplatform.dlorchestration;

import java.io.Serializable;
import java.sql.Timestamp;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "LeadScoringCommandId")
public class ModelCommandId implements HasPid, Serializable {

    private Long commandId;

    private Timestamp createTime;

    private String createdBy;

    private static final long serialVersionUID = 1L;

    @Id
    @Basic(optional = false)
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "CommandId", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return this.commandId;
    }

    @Override
    public void setPid(Long pid) {
        this.commandId = pid;
    }

    @Column(name = "CreateTime", nullable = false)
    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }

    @Column(name = "CreatedBy", nullable = false)
    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    ModelCommandId() {
        super();
    }

    @VisibleForTesting
    public ModelCommandId(Timestamp createTime, String createdBy) {
        super();
        this.createTime = createTime;
        this.createdBy = createdBy;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((commandId == null) ? 0 : commandId.hashCode());
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
        ModelCommandId other = (ModelCommandId) obj;
        if (commandId == null) {
            if (other.commandId != null)
                return false;
        } else if (!commandId.equals(other.commandId))
            return false;
        return true;
    }

}
