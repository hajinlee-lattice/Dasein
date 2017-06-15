package com.latticeengines.domain.exposed.metadata;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@javax.persistence.Table(name = "DATAFEED_IMPORT", uniqueConstraints = @UniqueConstraint(columnNames = { "SOURCE",
        "ENTITY", "FEED_TYPE", "FK_FEED_EXEC_ID" }))
public class DataFeedImport implements HasPid, Serializable {

    private static final long serialVersionUID = -6740417234916797093L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonIgnore
    @ManyToOne
    @JoinColumn(name = "`FK_FEED_EXEC_ID`", nullable = false)
    private DataFeedExecution execution;

    @Column(name = "SOURCE", nullable = false)
    @JsonProperty("source")
    private String source;

    @Column(name = "FEED_TYPE", nullable = true)
    @JsonProperty("feed_type")
    private String feedType;

    @Column(name = "ENTITY", nullable = false)
    @JsonProperty("entity")
    private String entity;

    @Column(name = "SOURCE_CONFIG", nullable = false, length = 1000)
    @JsonProperty("source_config")
    private String sourceConfig;

    @JsonProperty("data_table")
    @OneToOne
    @JoinColumn(name = "FK_DATA_ID", nullable = true)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Table dataTable;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "START_TIME", nullable = false)
    @JsonProperty("start_time")
    private Date startTime;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public DataFeedExecution getExecution() {
        return execution;
    }

    public void setExecution(DataFeedExecution execution) {
        this.execution = execution;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getFeedType() {
        return feedType;
    }

    public void setFeedType(String feedType) {
        this.feedType = feedType;
    }

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    public Table getDataTable() {
        return dataTable;
    }

    public void setDataTable(Table dataTable) {
        this.dataTable = dataTable;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public String getSourceConfig() {
        return sourceConfig;
    }

    public void setSourceConfig(String sourceConfig) {
        this.sourceConfig = sourceConfig;
    }
}
