package com.latticeengines.domain.exposed.propdata.manage;

import java.util.Date;
import java.util.List;

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

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.Index;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;

@Entity
@Access(AccessType.FIELD)
@Table(name = "MatchCommand")
public class MatchCommand implements HasPid {

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

    @Column(name = "Customer")
    protected String customer;

    @Column(name = "ColumnSelection")
    protected String columnSelection;

    @Column(name = "CreateTime")
    protected Date createTime = new Date();

    @Column(name = "LatestStatusUpdate")
    protected Date latestStatusUpdate;

    @Enumerated(EnumType.STRING)
    @Column(name = "StatusBeforeFailed", length = 20)
    protected MatchStatus statusBeforeFailed;

    @Column(name = "ErrorMessage")
    protected String errorMessage;

    @Column(name = "NumRetries")
    protected int numRetries;

    @Column(name = "ApplicationId")
    protected String applicationId;

    @Column(name = "Progress")
    protected Float progress;

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "matchCommand")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @Fetch(FetchMode.SUBSELECT)
    private List<MatchOperation> operationLogs;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getRootOperationUid() {
        return rootOperationUid;
    }

    public void setRootOperationUid(String rootOperationUid) {
        this.rootOperationUid = rootOperationUid;
    }

    public Integer getRowsRequested() {
        return rowsRequested;
    }

    public void setRowsRequested(Integer rowsRequested) {
        this.rowsRequested = rowsRequested;
    }

    public Integer getRowsMatched() {
        return rowsMatched;
    }

    public void setRowsMatched(Integer rowsMatched) {
        this.rowsMatched = rowsMatched;
    }

    public MatchStatus getMatchStatus() {
        return matchStatus;
    }

    public void setMatchStatus(MatchStatus matchStatus) {
        this.matchStatus = matchStatus;
    }

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    public String getColumnSelection() {
        return columnSelection;
    }

    public void setColumnSelection(String columnSelection) {
        this.columnSelection = columnSelection;
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

    public MatchStatus getStatusBeforeFailed() {
        return statusBeforeFailed;
    }

    public void setStatusBeforeFailed(MatchStatus statusBeforeFailed) {
        this.statusBeforeFailed = statusBeforeFailed;
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

    public List<MatchOperation> getOperationLogs() {
        return operationLogs;
    }

    public void setOperationLogs(List<MatchOperation> operationLogs) {
        this.operationLogs = operationLogs;
    }

}
