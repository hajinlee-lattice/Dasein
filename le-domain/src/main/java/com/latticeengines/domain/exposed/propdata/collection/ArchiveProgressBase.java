package com.latticeengines.domain.exposed.propdata.collection;

import java.util.Date;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.MappedSuperclass;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@MappedSuperclass
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
abstract public class ArchiveProgressBase implements HasPid {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ProgressID", unique = true, nullable = false)
    protected Long pid;

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
    protected ArchiveProgressStatus status;

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
    protected ArchiveProgressStatus statusBeforeFailed;

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

    public ArchiveProgressStatus getStatus() {
        return status;
    }

    public void setStatus(ArchiveProgressStatus status) {
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

    public ArchiveProgressStatus getStatusBeforeFailed() { return statusBeforeFailed; }

    public void setStatusBeforeFailed(ArchiveProgressStatus statusBeforeFailed) { this.statusBeforeFailed = statusBeforeFailed; }

    public String getErrorMessage() { return errorMessage; }

    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public int getNumRetries() { return numRetries; }

    public void setNumRetries(int numRetries) { this.numRetries = numRetries; }

    public static <T extends ArchiveProgressBase> T constructByDates(Date startDate, Date endDate, Class<T> tClass)
            throws InstantiationException, IllegalAccessException {
        T progress = tClass.newInstance();

        progress.setStartDate(startDate);
        progress.setEndDate(endDate);

        progress.setRootOperationUID(UUID.randomUUID().toString().toUpperCase());
        progress.setRowsDownloadedToHdfs(0);
        progress.setRowsUploadedToSql(0);
        progress.setStatus(ArchiveProgressStatus.NEW);

        return progress;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append(pid).append(startDate).append(endDate).append(rootOperationUID)
                .append(status).toString();
    }
}
