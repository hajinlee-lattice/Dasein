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
import javax.persistence.UniqueConstraint;

@Entity
@Access(AccessType.FIELD)
@Table(name = "RefreshProgress", uniqueConstraints = { @UniqueConstraint(columnNames = { "RootOperationUID" }) })
public class RefreshProgress implements Progress {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ProgressID", unique = true, nullable = false)
    protected Long pid;

    @Column(name = "SourceName", nullable = false)
    protected String sourceName;

    @Column(name = "PivotDate", nullable = false)
    protected Date pivotDate;

    @Column(name = "BaseSourceVersion", nullable = false)
    protected String baseSourceVersion;

    @Column(name = "RowsGenerated", nullable = false)
    protected long rowsGenerated;

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

    @Override
    public String getSourceName() { return sourceName; }

    public void setSourceName(String sourceName) { this.sourceName = sourceName; }

    public Date getPivotDate() {
        return pivotDate;
    }

    public void setPivotDate(Date pivotDate) {
        this.pivotDate = pivotDate;
    }

    public String getBaseSourceVersion() {
        return baseSourceVersion;
    }

    public void setBaseSourceVersion(String baseSourceVersion) {
        this.baseSourceVersion = baseSourceVersion;
    }

    public long getRowsGenerated() {
        return rowsGenerated;
    }

    public void setRowsGenerated(long rowsGenerated) {
        this.rowsGenerated = rowsGenerated;
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
    public Date getLatestStatusUpdate() { return latestStatusUpdate; }

    private void setLatestStatusUpdate(Date latestStatusUpdate) { this.latestStatusUpdate = latestStatusUpdate; }

    public String getCreatedBy() { return createdBy; }

    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }

    @Override
    public Date getCreateTime() { return createTime; }

    public void setCreateTime(Date createTime) { this.createTime = createTime; }

    @Override
    public ProgressStatus getStatusBeforeFailed() { return statusBeforeFailed; }

    @Override
    public void setStatusBeforeFailed(ProgressStatus statusBeforeFailed) { this.statusBeforeFailed = statusBeforeFailed; }

    public String getErrorMessage() { return errorMessage; }

    @Override
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    @Override
    public int getNumRetries() { return numRetries; }

    @Override
    public void setNumRetries(int numRetries) { this.numRetries = numRetries; }

    public static RefreshProgress constructByDate(String sourceName, Date pivotDate)
            throws InstantiationException, IllegalAccessException {
        RefreshProgress progress = new RefreshProgress();
        progress.setSourceName(sourceName);
        progress.setPivotDate(pivotDate);

        progress.setRootOperationUID(UUID.randomUUID().toString().toUpperCase());
        progress.setRowsGenerated(0);
        progress.setStatus(ProgressStatus.NEW);

        return progress;
    }

    @Override
    public String toString() {
        return String.format("RefreshProgress %s [%s]", sourceName, rootOperationUID);
    }
}
