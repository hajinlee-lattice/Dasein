package com.latticeengines.domain.exposed.propdata.manage;

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

@Entity
@Access(AccessType.FIELD)
@Table(name = "ArchiveProgress")
public class ArchiveProgress implements Progress {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ProgressID", unique = true, nullable = false)
    protected Long pid;

    @Column(name = "SourceName", nullable = false)
    protected String sourceName;

    @Column(name = "StartDate")
    protected Date startDate;

    @Column(name = "EndDate")
    protected Date endDate;

    @Column(name = "RowsDownloadedToHDFS")
    protected long rowsDownloadedToHdfs;

    @Column(name = "RowsUploadedToSQL")
    protected long rowsUploadedToSql;

    @Enumerated(EnumType.STRING)
    @Column(name = "Status")
    protected ProgressStatus status;

    @Column(name = "LatestStatusUpdate")
    protected Date latestStatusUpdate;

    @Column(name = "RootOperationUID", unique = true, nullable = false)
    protected String rootOperationUID;

    @Column(name = "CreatedBy")
    protected String createdBy;

    @Column(name = "CreateTime")
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

    @Override
    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

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

    @Override
    public ProgressStatus getStatus() {
        return status;
    }

    @Override
    public void setStatus(ProgressStatus status) {
        this.status = status;
        setLatestStatusUpdate(new Date());
    }

    @Override
    public String getRootOperationUID() {
        return rootOperationUID;
    }

    public void setRootOperationUID(String rootOperationUID) {
        this.rootOperationUID = rootOperationUID;
    }

    @Override
    public Date getLatestStatusUpdate() {
        return latestStatusUpdate;
    }

    private void setLatestStatusUpdate(Date latestStatusUpdate) {
        this.latestStatusUpdate = latestStatusUpdate;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    @Override
    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    @Override
    public ProgressStatus getStatusBeforeFailed() {
        return statusBeforeFailed;
    }

    @Override
    public void setStatusBeforeFailed(ProgressStatus statusBeforeFailed) {
        this.statusBeforeFailed = statusBeforeFailed;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public int getNumRetries() {
        return numRetries;
    }

    @Override
    public void setNumRetries(int numRetries) {
        this.numRetries = numRetries;
    }

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
        return String.format("ArchiveProgress %s [%s]", sourceName, rootOperationUID);
    }
}
