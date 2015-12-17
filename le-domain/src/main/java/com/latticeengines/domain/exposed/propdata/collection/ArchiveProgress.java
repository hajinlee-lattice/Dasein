package com.latticeengines.domain.exposed.propdata.collection;

import java.util.Date;
import java.util.UUID;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "ArchiveProgress", uniqueConstraints = { @UniqueConstraint(columnNames = { "RootOperationUID" }) })
public class ArchiveProgress implements HasPid {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ProgressID", unique = true, nullable = false)
    protected Long pid;

    @Column(name = "SourceName", nullable = false)
    protected String sourceName;

    @Column(name = "StartDate", nullable = false)
    protected Date startDate;

    @Column(name = "EndDate", nullable = false)
    protected Date endDate;

    @Column(name = "RowsDownloadedToHDFS", nullable = false)
    protected long rowsDownloadedToHdfs;

    @Column(name = "RowsUploadedToSQL", nullable = false)
    protected long rowsUploadedToSql;

    @Enumerated(EnumType.STRING)
    @Column(name = "Status", nullable = false)
    protected ProgressStatus status;

    @Column(name = "LatestStatusUpdate", nullable = false)
    protected Date latestStatusUpdate;

    @Column(name = "RootOperationUID", unique = true, nullable = false)
    protected String rootOperationUID;

    @Column(name = "CreatedBy", nullable = false)
    protected String createdBy;

    @Column(name = "CreateTime", nullable = false)
    protected Date createTime = new Date();

    @Enumerated(EnumType.STRING)
    @Column(name = "StatusBeforeFailed")
    protected ProgressStatus statusBeforeFailed;

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

    public String getSourceName() { return sourceName; }

    public void setSourceName(String sourceName) { this.sourceName = sourceName; }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public long getRowsDownloadedToHdfs() {
        return rowsDownloadedToHdfs;
    }

    public void setRowsDownloadedToHdfs(long rowsDownloadedToHdfs) {
        this.rowsDownloadedToHdfs = rowsDownloadedToHdfs;
    }

    public long getRowsUploadedToSql() {
        return rowsUploadedToSql;
    }

    public void setRowsUploadedToSql(long rowsUploadedToSql) {
        this.rowsUploadedToSql = rowsUploadedToSql;
    }

    public ProgressStatus getStatus() {
        return status;
    }

    public void setStatus(ProgressStatus status) {
        this.status = status;
        setLatestStatusUpdate(new Date());
    }

    public String getRootOperationUID() {
        return rootOperationUID;
    }

    public void setRootOperationUID(String rootOperationUID) {
        this.rootOperationUID = rootOperationUID;
    }

    public Date getLatestStatusUpdate() { return latestStatusUpdate; }

    private void setLatestStatusUpdate(Date latestStatusUpdate) { this.latestStatusUpdate = latestStatusUpdate; }

    public String getCreatedBy() { return createdBy; }

    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }

    public Date getCreateTime() { return createTime; }

    public void setCreateTime(Date createTime) { this.createTime = createTime; }

    public ProgressStatus getStatusBeforeFailed() { return statusBeforeFailed; }

    public void setStatusBeforeFailed(ProgressStatus statusBeforeFailed) { this.statusBeforeFailed = statusBeforeFailed; }

    public String getErrorMessage() { return errorMessage; }

    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public int getNumRetries() { return numRetries; }

    public void setNumRetries(int numRetries) { this.numRetries = numRetries; }

    public static ArchiveProgress constructByDates(String sourceName, Date startDate, Date endDate)
            throws InstantiationException, IllegalAccessException {
        ArchiveProgress progress = new ArchiveProgress();
        progress.setSourceName(sourceName);
        progress.setStartDate(startDate);
        progress.setEndDate(endDate);

        progress.setRootOperationUID(UUID.randomUUID().toString().toUpperCase());
        progress.setRowsDownloadedToHdfs(0);
        progress.setRowsUploadedToSql(0);
        progress.setStatus(ProgressStatus.NEW);

        return progress;
    }

    @Override
    public String toString() {
        return String.format("%s [%s]", sourceName, rootOperationUID);
    }
}
