package com.latticeengines.domain.exposed.dataplatform.dlorchestration;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.Parameter;
import org.hibernate.annotations.Type;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "LeadScoringResult")
public class ModelCommandResult implements HasPid, Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @Column(name = "CommandId", unique = true, nullable = false)
    private Long commandId;

    @OneToOne(fetch = FetchType.LAZY)
    @PrimaryKeyJoinColumn
    private ModelCommand modelCommand;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "BeginTime", nullable = false)
    private Date beginTime;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "EndTime", nullable = false)
    private Date endTime;

    @Column(name = "ProcessStatus", nullable = false)
    @Type(type = "com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatusUserType", parameters = {
            @Parameter(name = "enumClassName", value = "com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus"),
            @Parameter(name = "identifierMethod", value = "getValue"),
            @Parameter(name = "valueOfMethod", value = "valueOf") })
    private ModelCommandStatus processStatus;

    ModelCommandResult() {
        super();
    }

    public ModelCommandResult(ModelCommand modelCommand, Date beginTime, Date endTime,
            ModelCommandStatus processStatus) {
        super();
        this.commandId = modelCommand.getPid();
        this.modelCommand = modelCommand;
        this.beginTime = beginTime;
        this.endTime = endTime;
        this.processStatus = processStatus;
    }

    @Override
    public Long getPid() {
        return this.commandId;
    }

    @Override
    public void setPid(Long id) {
        this.commandId = id;
    }

    public ModelCommand getModelCommand() {
        return modelCommand;
    }

    public Date getBeginTime() {
        return beginTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public ModelCommandStatus getProcessStatus() {
        return processStatus;
    }

    public void setProcessStatus(ModelCommandStatus processStatus) {
        this.processStatus = processStatus;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((beginTime == null) ? 0 : beginTime.hashCode());
        result = prime * result + ((endTime == null) ? 0 : endTime.hashCode());
        result = prime * result + ((modelCommand == null) ? 0 : modelCommand.hashCode());
        result = prime * result + ((processStatus == null) ? 0 : processStatus.hashCode());
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
        ModelCommandResult other = (ModelCommandResult) obj;
        if (beginTime == null) {
            if (other.beginTime != null)
                return false;
        } else if (!beginTime.equals(other.beginTime))
            return false;
        if (endTime == null) {
            if (other.endTime != null)
                return false;
        } else if (!endTime.equals(other.endTime))
            return false;
        if (modelCommand == null) {
            if (other.modelCommand != null)
                return false;
        } else if (!modelCommand.equals(other.modelCommand))
            return false;
        if (processStatus != other.processStatus)
            return false;
        return true;
    }

}
