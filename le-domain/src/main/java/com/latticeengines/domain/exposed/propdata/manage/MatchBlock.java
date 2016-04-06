package com.latticeengines.domain.exposed.propdata.manage;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.hibernate.annotations.Index;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "MatchBlock")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MatchBlock implements HasPid {

    private static Log log = LogFactory.getLog(MatchCommand.class);
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS z";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
    private static Calendar calendar = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));

    static {
        formatter.setCalendar(calendar);
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", nullable = false)
    private Long pid;

    @Index(name = "IX_UID")
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

    @JsonProperty("NumRows")
    public Integer getNumRows() {
        return numRows;
    }

    @JsonProperty("NumRows")
    public void setNumRows(Integer numRows) {
        this.numRows = numRows;
    }

    @JsonProperty("ApplicationState")
    public YarnApplicationState getApplicationState() {
        return applicationState;
    }

    @JsonProperty("ApplicationState")
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

    @JsonProperty("StateBeforeFailed")
    public YarnApplicationState getStateBeforeFailed() {
        return stateBeforeFailed;
    }

    @JsonProperty("StateBeforeFailed")
    public void setStateBeforeFailed(YarnApplicationState stateBeforeFailed) {
        this.stateBeforeFailed = stateBeforeFailed;
    }

    @JsonProperty("ErrorMessage")
    public String getErrorMessage() {
        return errorMessage;
    }

    @JsonProperty("ErrorMessage")
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @JsonProperty("NumRetries")
    public int getNumRetries() {
        return numRetries;
    }

    @JsonProperty("NumRetries")
    public void setNumRetries(int numRetries) {
        this.numRetries = numRetries;
    }

    @JsonProperty("ApplicationId")
    public String getApplicationId() {
        return applicationId;
    }

    @JsonProperty("ApplicationId")
    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    @JsonProperty("Progress")
    public Float getProgress() {
        return progress;
    }

    @JsonProperty("Progress")
    public void setProgress(Float progress) {
        this.progress = progress;
    }

    @JsonProperty("CreateTime")
    private String getCreateTimeAsString() {
        return createTime == null ? null : formatter.format(createTime);
    }

    @JsonProperty("CreateTime")
    private void setCreateTimeByString(String createTimeString) {
        try {
            SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
            formatter.setCalendar(calendar);
            this.createTime = formatter.parse(createTimeString);
        } catch (Exception e) {
            log.error("Failed to parse timestamp ReceviedAt [" + createTimeString + "]", e);
            this.createTime = null;
        }
    }

    @JsonProperty("LatestStatusUpdate")
    private String getLatestStatusUpdateAsString() {
        return latestStatusUpdate == null ? null : formatter.format(latestStatusUpdate);
    }

    @JsonProperty("LatestStatusUpdate")
    private void setLatestStatusUpdateByString(String latestStatusUpdateString) {
        try {
            SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
            formatter.setCalendar(calendar);
            this.latestStatusUpdate = formatter.parse(latestStatusUpdateString);
        } catch (Exception e) {
            log.error("Failed to parse timestamp ReceviedAt [" + latestStatusUpdateString + "]", e);
            this.latestStatusUpdate = null;
        }
    }
}
