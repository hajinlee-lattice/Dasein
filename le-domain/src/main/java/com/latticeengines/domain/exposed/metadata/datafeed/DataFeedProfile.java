package com.latticeengines.domain.exposed.metadata.datafeed;

import java.io.Serializable;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@javax.persistence.Table(name = "DATAFEED_PROFILE")
public class DataFeedProfile implements HasPid, Serializable {

    private static final long serialVersionUID = 5167450463065928362L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_FEED_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonIgnore
    private DataFeed dataFeed;

    @JsonProperty("feed_execution_id")
    @Column(name = "FEED_EXEC_ID", nullable = false)
    private Long latestDataFeedExecutionId;

    @JsonProperty("workflow_id")
    @Column(name = "WORKFLOW_ID", nullable = true)
    private Long workflowId;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public DataFeed getDataFeed() {
        return dataFeed;
    }

    public void setDataFeed(DataFeed feed) {
        this.dataFeed = feed;
    }

    public Long getLatestDataFeedExecutionId() {
        return latestDataFeedExecutionId;
    }

    public void setLatestDataFeedExecutionId(Long latestDataFeedExecutionId) {
        this.latestDataFeedExecutionId = latestDataFeedExecutionId;
    }

    public Long getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(Long workflowId) {
        this.workflowId = workflowId;
    }
}
