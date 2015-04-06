package com.latticeengines.domain.exposed.scoring;

import java.sql.Timestamp;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@Table(name = "LeadInputQueue")
public class LeadInputQueue {

    private Integer leadInputQueueId;
    private String leDeploymentdId;
    private String tableName;
    private Integer total;
    private Integer lower;
    private Integer status;
    private Timestamp populated;
    private Timestamp consumed;
    
    
    @Id
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "LeadInputQueue_ID", unique = true, nullable = false)
    public Integer getLeadInputQueueId() {
        return leadInputQueueId;
    }
    
    public void setLeadInputQueueId(Integer leadInputQueueId) {
        this.leadInputQueueId = leadInputQueueId;
    }

    @Column(name = "LEDeployment_ID", nullable = true)
    public String getLeDeploymentdId() {
        return leDeploymentdId;
    }

    public void setLeDeploymentdId(String leDeploymentdId) {
        this.leDeploymentdId = leDeploymentdId;
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
    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
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
    
    
}
