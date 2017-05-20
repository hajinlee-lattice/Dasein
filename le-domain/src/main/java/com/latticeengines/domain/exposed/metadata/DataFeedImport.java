package com.latticeengines.domain.exposed.metadata;

import java.io.Serializable;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.UniqueConstraint;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@javax.persistence.Table(name = "DATAFEED_IMPORT", uniqueConstraints = @UniqueConstraint(columnNames = { "EXECUTION_ID",
        "SOURCE", "ENTITY" }))
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
    @JoinColumn(name = "FK_FEED_EXEC_ID", nullable = false)
    private DataFeedExecution execution;

    @Column(name = "EXECUTION_ID", nullable = false)
    @JsonProperty("executionId")
    private Long executionId;

    @Column(name = "SOURCE", nullable = false)
    @JsonProperty("source")
    private String source;

    @Column(name = "ENTITY", nullable = false)
    @JsonProperty("entity")
    private String entity;

    @Column(name = "SOURCE_CONFIG", nullable = false)
    @JsonProperty("sourceConfig")
    private String sourceConfig;

    @JsonIgnore
    @OneToOne
    @JoinColumn(name = "FK_DATA_ID", nullable = false)
    private Table data;

    @Column(name = "START_TIME", nullable = false)
    @JsonProperty("StartTime")
    private Long startTime;

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

    public Long getExecutionId() {
        return executionId;
    }

    public void setExecutionId(Long executionId) {
        this.executionId = executionId;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    public Table getData() {
        return data;
    }

    public void setData(Table data) {
        this.data = data;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }
}
