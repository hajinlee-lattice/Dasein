package com.latticeengines.domain.exposed.modelquality;

import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.UniqueConstraint;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;

/**
 *
 * @startuml
 *
 */
@Entity
@Table(name = "MODELQUALITY_MODELRUN", uniqueConstraints = { @UniqueConstraint(columnNames = { "NAME" }) })
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class ModelRun implements HasName, HasPid, Fact, Dimension, HasAuditingFields {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "NAME", nullable = false)
    private String name;

    @ManyToOne
    @JoinColumn(name = "FK_ANALYTIC_PIPELINE_ID", nullable = false)
    @JsonIgnore
    private AnalyticPipeline analyticPipeline;

    @ManyToOne
    @JoinColumn(name = "FK_DATASET_ID", nullable = false)
    @JsonIgnore
    private DataSet dataSet;

    @Column(name = "STATUS", nullable = false)
    private ModelRunStatus status;

    @Column(name = "CREATION_DATE", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date creationDate;

    @Column(name = "UPDATE_DATE", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updateDate;

    @Column(name = "DESCRIPTION", length = 4000)
    private String description;

    @Column(name = "ERROR_MESSAGE", length = 4000)
    private String errorMessage;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public AnalyticPipeline getAnalyticPipeline() {
        return analyticPipeline;
    }

    public void setAnalyticPipeline(AnalyticPipeline analyticPipeline) {
        this.analyticPipeline = analyticPipeline;
    }

    public DataSet getDataSet() {
        return dataSet;
    }

    public void setDataSet(DataSet dataSet) {
        this.dataSet = dataSet;
    }

    public ModelRunStatus getStatus() {
        return status;
    }

    public void setStatus(ModelRunStatus status) {
        this.status = status;
    }

    @Override
    public Date getCreated() {
        return creationDate;
    }

    @Override
    public void setCreated(Date date) {
        this.creationDate = date;
    }

    @Override
    public Date getUpdated() {
        return updateDate;
    }

    @Override
    public void setUpdated(Date date) {
        this.updateDate = date;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
