package com.latticeengines.domain.exposed.propdata.manage;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

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
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.Index;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;

@Entity
@Access(AccessType.FIELD)
@Table(name = "MatchCommand")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MatchCommand implements HasPid {

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
    @Column(name = "RootOperationUID", unique = true, nullable = false, length = 100)
    private String rootOperationUid;

    @Column(name = "RowsRequested", nullable = false)
    private Integer rowsRequested;

    @Column(name = "RowsMatched")
    private Integer rowsMatched;

    @Enumerated(EnumType.STRING)
    @Column(name = "MatchStatus", nullable = false, length = 20)
    private MatchStatus matchStatus;

    @Column(name = "Customer", length=200)
    protected String customer;

    @Column(name = "ColumnSelection", length=50)
    protected String columnSelection;

    @Column(name = "CreateTime")
    protected Date createTime = new Date();

    @Column(name = "LatestStatusUpdate")
    protected Date latestStatusUpdate;

    @Enumerated(EnumType.STRING)
    @Column(name = "StatusBeforeFailed", length = 20)
    protected MatchStatus statusBeforeFailed;

    @Column(name = "ErrorMessage", length=1000)
    protected String errorMessage;

    @Column(name = "NumRetries")
    protected int numRetries;

    @Column(name = "ApplicationId", length=50)
    protected String applicationId;

    @Column(name = "Progress")
    protected Float progress;

    @Column(name = "ResultLocation", length=400)
    protected String resultLocation;


    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER, mappedBy = "matchCommand")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @Fetch(FetchMode.SUBSELECT)
    private List<MatchBlock> matchBlocks;

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

    @JsonProperty("MatchStatus")
    public MatchStatus getMatchStatus() {
        return matchStatus;
    }

    @JsonProperty("MatchStatus")
    public void setMatchStatus(MatchStatus matchStatus) {
        this.matchStatus = matchStatus;
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
