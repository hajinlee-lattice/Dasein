package com.latticeengines.domain.exposed.datacloud.manage;

import java.util.Date;
import java.util.Map;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchResult;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "MatchBlock", indexes = { @Index(name = "IX_UID", columnList = "BlockOperationUID") })
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MatchBlock implements HasPid {

    @JsonProperty("ApplicationId")
    @Column(name = "ApplicationId")
    protected String applicationId;
    @JsonProperty("Progress")
    @Column(name = "Progress")
    protected Float progress;
    @JsonProperty("CreateTime")
    @Column(name = "CreateTime")
    protected Date createTime = new Date();
    @JsonProperty("LatestStatusUpdate")
    @Column(name = "LatestStatusUpdate")
    protected Date latestStatusUpdate;
    @JsonProperty("StateBeforeFailed")
    @Enumerated(EnumType.STRING)
    @Column(name = "StateBeforeFailed", length = 20)
    protected YarnApplicationState stateBeforeFailed;
    @JsonProperty("ErrorMessage")
    @Column(name = "ErrorMessage")
    protected String errorMessage;
    @JsonProperty("NumRetries")
    @Column(name = "NumRetries")
    protected int numRetries;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", nullable = false)
    private Long pid;
    @Column(name = "BlockOperationUID", nullable = false, length = 100)
    private String blockOperationUid;
    @ManyToOne(cascade = { CascadeType.MERGE })
    @JoinColumn(name = "FK_MatchCommand", nullable = false)
    private MatchCommand matchCommand;
    @JsonProperty("NumRows")
    @Column(name = "NumRows", nullable = false)
    private Integer numRows;
    @JsonProperty("MatchedRows")
    @Column(name = "MatchedRows")
    private Integer matchedRows;
    @JsonProperty("MatchResults")
    @Column(name = "MatchResults", columnDefinition = "'JSON'")
    @org.hibernate.annotations.Type(type = "json")
    private Map<EntityMatchResult, Long> matchResults;
    @JsonProperty("ApplicationState")
    @Enumerated(EnumType.STRING)
    @Column(name = "ApplicationState", nullable = false, length = 20)
    private YarnApplicationState applicationState;

    @Override
    @JsonIgnore
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @JsonProperty("BlockOperationUID")
    public String getBlockOperationUid() {
        return blockOperationUid;
    }

    @JsonProperty("BlockOperationUID")
    public void setBlockOperationUid(String blockOperationUid) {
        this.blockOperationUid = blockOperationUid;
    }

    @JsonIgnore
    public MatchCommand getMatchCommand() {
        return matchCommand;
    }

    @JsonIgnore
    public void setMatchCommand(MatchCommand matchCommand) {
        this.matchCommand = matchCommand;
    }

    public Integer getNumRows() {
        return numRows;
    }

    public void setNumRows(Integer numRows) {
        this.numRows = numRows;
    }

    public Integer getMatchedRows() {
        return matchedRows;
    }

    public void setMatchedRows(Integer matchedRows) {
        this.matchedRows = matchedRows;
    }

    public Map<EntityMatchResult, Long> getMatchResults() {
        return matchResults;
    }

    public void setMatchResults(Map<EntityMatchResult, Long> matchResults) {
        this.matchResults = matchResults;
    }

    public YarnApplicationState getApplicationState() {
        return applicationState;
    }

    public void setApplicationState(YarnApplicationState applicationState) {
        this.applicationState = applicationState;
    }

    @JsonIgnore
    public Date getCreateTime() {
        return createTime;
    }

    @JsonIgnore
    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    @JsonIgnore
    public Date getLatestStatusUpdate() {
        return latestStatusUpdate;
    }

    @JsonIgnore
    public void setLatestStatusUpdate(Date latestStatusUpdate) {
        this.latestStatusUpdate = latestStatusUpdate;
    }

    public YarnApplicationState getStateBeforeFailed() {
        return stateBeforeFailed;
    }

    public void setStateBeforeFailed(YarnApplicationState stateBeforeFailed) {
        this.stateBeforeFailed = stateBeforeFailed;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public int getNumRetries() {
        return numRetries;
    }

    public void setNumRetries(int numRetries) {
        this.numRetries = numRetries;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public Float getProgress() {
        return progress;
    }

    public void setProgress(Float progress) {
        this.progress = progress;
    }

    @SuppressWarnings("unused")
    private String getCreateTimeAsString() {
        return DateTimeUtils.format(createTime);
    }

    @SuppressWarnings("unused")
    private void setCreateTimeByString(String createTimeString) {
        this.createTime = DateTimeUtils.parse(createTimeString);
    }

    @SuppressWarnings("unused")
    private String getLatestStatusUpdateAsString() {
        return DateTimeUtils.format(latestStatusUpdate);
    }

    @SuppressWarnings("unused")
    private void setLatestStatusUpdateByString(String latestStatusUpdateString) {
        this.latestStatusUpdate = DateTimeUtils.parse(latestStatusUpdateString);
    }
}
