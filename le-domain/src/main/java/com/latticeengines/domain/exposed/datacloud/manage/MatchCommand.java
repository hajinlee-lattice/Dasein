package com.latticeengines.domain.exposed.datacloud.manage;

import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchResult;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "MatchCommand", indexes = {
        @Index(name = "IX_UID", columnList = "RootOperationUID") })
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MatchCommand implements HasPid {

    @Column(name = "Customer", length = 200)
    protected String customer;
    @Column(name = "ColumnSelection", length = 50)
    protected String columnSelection;
    @Column(name = "CreateTime")
    protected Date createTime = new Date();
    @Column(name = "LatestStatusUpdate")
    protected Date latestStatusUpdate;
    @Enumerated(EnumType.STRING)
    @Column(name = "StatusBeforeFailed", length = 20)
    protected MatchStatus statusBeforeFailed;
    @Column(name = "ErrorMessage", length = 1000)
    protected String errorMessage;
    @Column(name = "NumRetries")
    protected int numRetries;
    @Column(name = "ApplicationId", length = 50)
    protected String applicationId;
    @Column(name = "Progress")
    protected Float progress;
    @Column(name = "ResultLocation", length = 400)
    protected String resultLocation;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", nullable = false)
    private Long pid;
    @Column(name = "RootOperationUID", unique = true, nullable = false, length = 100)
    private String rootOperationUid;
    @Column(name = "RowsRequested", nullable = false)
    private Integer rowsRequested;
    @Column(name = "RowsMatched")
    private Integer rowsMatched;
    @JsonProperty("MatchResults")
    @Column(name = "MatchResults", columnDefinition = "'JSON'")
    @org.hibernate.annotations.Type(type = "json")
    private Map<EntityMatchResult, Long> matchResults;
    @Column(name = "RowsToDnB")
    private Integer rowsToDnb = 0;
    @Column(name = "RowsMatchedByDnB")
    private Integer rowsMatchedByDnb = 0;
    @Column(name = "DnBDurationAvg")
    private Integer dnbDurationAvg = 0;
    @Enumerated(EnumType.STRING)
    @Column(name = "MatchStatus", nullable = false, length = 20)
    private MatchStatus matchStatus;
    @Enumerated(EnumType.STRING)
    @Column(name = "JobType", nullable = false, length = 20)
    private MatchRequestSource jobType;
    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER, mappedBy = "matchCommand")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @Fetch(FetchMode.SUBSELECT)
    private List<MatchBlock> matchBlocks;

    @Transient
    private String cascadingFlow;

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

    @JsonProperty("RootOperationUID")
    public String getRootOperationUid() {
        return rootOperationUid;
    }

    @JsonProperty("RootOperationUID")
    public void setRootOperationUid(String rootOperationUid) {
        this.rootOperationUid = rootOperationUid;
    }

    @JsonProperty("RowsRequested")
    public Integer getRowsRequested() {
        return rowsRequested;
    }

    @JsonProperty("RowsRequested")
    public void setRowsRequested(Integer rowsRequested) {
        this.rowsRequested = rowsRequested;
    }

    @JsonProperty("RowsMatched")
    public Integer getRowsMatched() {
        return rowsMatched;
    }

    @JsonProperty("RowsMatched")
    public void setRowsMatched(Integer rowsMatched) {
        this.rowsMatched = rowsMatched;
    }

    public Map<EntityMatchResult, Long> getMatchResults() {
        return matchResults;
    }

    public void setMatchResults(Map<EntityMatchResult, Long> matchResults) {
        this.matchResults = matchResults;
    }

    @JsonProperty("RowsToDnB")
    public Integer getRowsToDnb() {
        return rowsToDnb;
    }

    @JsonProperty("RowsToDnB")
    public void setRowsToDnb(Integer rowsToDnb) {
        this.rowsToDnb = rowsToDnb;
    }

    @JsonProperty("RowsMatchedByDnB")
    public Integer getRowsMatchedByDnb() {
        return rowsMatchedByDnb;
    }

    @JsonProperty("RowsMatchedByDnB")
    public void setRowsMatchedByDnb(Integer rowsMatchedByDnb) {
        this.rowsMatchedByDnb = rowsMatchedByDnb;
    }

    @JsonProperty("DnBDurationAvg")
    public Integer getDnbDurationAvg() {
        return dnbDurationAvg;
    }

    @JsonProperty("DnBDurationAvg")
    public void setDnbDurationAvg(Integer dnbDurationAvg) {
        this.dnbDurationAvg = dnbDurationAvg;
    }

    @JsonProperty("MatchStatus")
    public MatchStatus getMatchStatus() {
        return matchStatus;
    }

    @JsonProperty("MatchStatus")
    public void setMatchStatus(MatchStatus matchStatus) {
        this.matchStatus = matchStatus;
    }

    @JsonProperty("JobType")
    public MatchRequestSource getJobType() {
        return jobType;
    }

    @JsonProperty("JobType")
    public void setJobType(MatchRequestSource jobType) {
        this.jobType = jobType;
    }

    @JsonProperty("Customer")
    public String getCustomer() {
        return customer;
    }

    @JsonProperty("Customer")
    public void setCustomer(String customer) {
        this.customer = customer;
    }

    @JsonProperty("ColumnSelection")
    public String getColumnSelection() {
        return columnSelection;
    }

    @JsonProperty("ColumnSelection")
    public void setColumnSelection(String columnSelection) {
        this.columnSelection = columnSelection;
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

    @JsonProperty("StatusBeforeFailed")
    public MatchStatus getStatusBeforeFailed() {
        return statusBeforeFailed;
    }

    @JsonProperty("StatusBeforeFailed")
    public void setStatusBeforeFailed(MatchStatus statusBeforeFailed) {
        this.statusBeforeFailed = statusBeforeFailed;
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

    @JsonProperty("MatchBlocks")
    public List<MatchBlock> getMatchBlocks() {
        return matchBlocks;
    }

    @JsonProperty("MatchBlocks")
    public void setMatchBlocks(List<MatchBlock> matchBlocks) {
        this.matchBlocks = matchBlocks;
    }

    @JsonProperty("ResultLocation")
    public String getResultLocation() {
        return resultLocation;
    }

    @JsonProperty("ResultLocation")
    public void setResultLocation(String resultLocation) {
        this.resultLocation = resultLocation;
    }

    @JsonProperty("CreateTime")
    private String getCreateTimeAsString() {
        return DateTimeUtils.format(createTime);
    }

    @JsonProperty("CreateTime")
    private void setCreateTimeByString(String createTimeString) {
        this.createTime = DateTimeUtils.parse(createTimeString);
    }

    @JsonProperty("LatestStatusUpdate")
    private String getLatestStatusUpdateAsString() {
        return DateTimeUtils.format(latestStatusUpdate);
    }

    @JsonProperty("LatestStatusUpdate")
    private void setLatestStatusUpdateByString(String latestStatusUpdateString) {
        this.latestStatusUpdate = DateTimeUtils.parse(latestStatusUpdateString);
    }

    @JsonProperty("CascadingFlow")
    public String getCascadingFlow() {
        return cascadingFlow;
    }

    @JsonProperty("CascadingFlow")
    public void setCascadingFlow(String cascadingFlow) {
        this.cascadingFlow = cascadingFlow;
    }

}
