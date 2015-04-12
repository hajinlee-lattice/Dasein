package com.latticeengines.domain.exposed.scoring;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.Parameter;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@Table(name = "LeadInputQueue")
public class ScoringCommand implements HasPid, HasId<String>, Serializable{

    private Long leadInputQueueId;
    private String leDeploymentId;
    private String tableName;
    private Integer total;
    private Integer lower;
    private ScoringCommandStatus status;
    private Timestamp populated;
    private Timestamp consumed;

    private static final long serialVersionUID = 1L;

    @Id
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "LeadInputQueue_ID", unique = true, nullable = false)
    public Long getPid() {
        return leadInputQueueId;
    }

    public void setPid(Long leadInputQueueId) {
        this.leadInputQueueId = leadInputQueueId;
    }

    @Column(name = "LEDeployment_ID", nullable = true)
    public String getId() {
        return leDeploymentId;
    }

    public void setId(String leDeploymentdId) {
        this.leDeploymentId = leDeploymentdId;
    }

    @Column(name = "Table_Name", nullable = false)
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Column(name = "Total", nullable = false)
    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    @Column(name = "Lower", nullable = false)
    public Integer getLower() {
        return lower;
    }

    public void setLower(Integer lower) {
        this.lower = lower;
    }

    @Column(name = "Status", nullable = false)
    @Type(type = "com.latticeengines.domain.exposed.scoring.ScoringCommandStatusUserType", parameters = {
            @Parameter(name = "enumClassName", value = "com.latticeengines.domain.exposed.scoring.ScoringCommandStatus"),
            @Parameter(name = "identifierMethod", value = "getValue"),
            @Parameter(name = "valueOfMethod", value = "valueOf") })
    public ScoringCommandStatus getStatus() {
        return status;
    }

    public void setStatus(ScoringCommandStatus status) {
        this.status = status;
    }

    @Column(name = "Populated", nullable = false)
    public Timestamp getPopulated() {
        return populated;
    }

    public void setPopulated(Timestamp populated) {
        this.populated = populated;
    }

    @Column(name = "Consumed", nullable = true)
    public Timestamp getConsumed() {
        return consumed;
    }

    public void setConsumed(Timestamp consumed) {
        this.consumed = consumed;
    }

    ScoringCommand(){
        super();
    }

    @VisibleForTesting
    public ScoringCommand(Long pid, String deploymentExternalId, ScoringCommandStatus status, String tableName, int lower, int total, Timestamp populated) {
        super();
        this.leadInputQueueId = pid;
        this.leDeploymentId = deploymentExternalId;
        this.status = status;
        this.tableName = tableName;
        this.lower = lower;
        this.total = total;
        this.populated = populated;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((leadInputQueueId == null) ? 0 : leadInputQueueId.hashCode());
        result = prime * result + ((populated == null) ? 0 : populated.hashCode());
        result = prime * result + ((consumed == null) ? 0 : consumed.hashCode());
        result = prime * result + ((status == null) ? 0 : status.hashCode());
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
        ScoringCommand other = (ScoringCommand) obj;
        if (populated == null) {
            if (other.populated != null)
                return false;
        } else if (!populated.equals(other.populated))
            return false;
        if (consumed == null) {
            if (other.consumed != null)
                return false;
        } else if (!consumed.equals(other.consumed))
            return false;
        if (leadInputQueueId == null) {
            if (other.leadInputQueueId != null)
                return false;
        } else if (!leadInputQueueId.equals(other.leadInputQueueId))
            return false;
        if (status != other.status)
            return false;
        return true;
    }

}
