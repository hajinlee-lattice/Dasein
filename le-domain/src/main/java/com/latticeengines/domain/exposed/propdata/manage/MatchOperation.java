package com.latticeengines.domain.exposed.propdata.manage;

import java.util.Date;

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
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "MatchOperation")
public class MatchOperation implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", nullable = false)
    private Long pid;

    @Column(name = "BlockOperationUID", nullable = false, length = 100)
    private String blockOperationUid;

    @ManyToOne(cascade = { CascadeType.MERGE, CascadeType.REMOVE })
    @JoinColumn(name = "RootOperationUID", nullable = false)
    private MatchCommand matchCommand;

    @Column(name = "NumRows", nullable = false)
    private Integer numRows;

    @Column(name = "ApplicationId")
    protected String applicationId;

    @Column(name = "Progress")
    protected Float progress;

    @Enumerated(EnumType.STRING)
    @Column(name = "ApplicationState", nullable = false, length = 20)
    private YarnApplicationState applicationState;

    @Column(name = "CreateTime")
    protected Date createTime = new Date();

    @Column(name = "LatestStatusUpdate")
    protected Date latestStatusUpdate;

    @Enumerated(EnumType.STRING)
    @Column(name = "StateBeforeFailed", length = 20)
    protected YarnApplicationState stateBeforeFailed;

    @Column(name = "ErrorMessage")
    protected String errorMessage;

    @Column(name = "NumRetries")
    protected int numRetries;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getBlockOperationUid() {
        return blockOperationUid;
    }

    public void setBlockOperationUid(String blockOperationUid) {
        this.blockOperationUid = blockOperationUid;
    }

    public MatchCommand getMatchCommand() {
        return matchCommand;
    }

    public void setMatchCommand(MatchCommand matchCommand) {
        this.matchCommand = matchCommand;
    }

    public Integer getNumRows() {
        return numRows;
    }

    public void setNumRows(Integer numRows) {
        this.numRows = numRows;
    }

    public YarnApplicationState getApplicationState() {
        return applicationState;
    }

    public void setApplicationState(YarnApplicationState applicationState) {
        this.applicationState = applicationState;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getLatestStatusUpdate() {
        return latestStatusUpdate;
    }

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
}
