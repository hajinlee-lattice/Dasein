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

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "TransformationProgress")
public class TransformationProgress implements HasPid {
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

    @Enumerated(EnumType.STRING)
    @Column(name = "Status")
    protected TransformationProgressStatus status;

    @Column(name = "LatestStatusUpdate")
    protected Date latestStatusUpdate;

    @Column(name = "RootOperationUID", unique = true, nullable = false)
    protected String rootOperationUID;

    @Column(name = "CreatedBy")
    protected String createdBy;

    @Column(name = "CreateTime")
    protected Date createTime = new Date();

    @Column(name = "ErrorMessage")
    protected String errorMessage;

    @Column(name = "NumRetries")
    protected int numRetries;

    @Column(name = "Version")
    protected String version;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

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

    public TransformationProgressStatus getStatus() {
        return status;
    }

    public void setStatus(TransformationProgressStatus status) {
        this.status = status;
        setLatestStatusUpdate(new Date());
    }

    public String getRootOperationUID() {
        return rootOperationUID;
    }

    public void setRootOperationUID(String rootOperationUID) {
        this.rootOperationUID = rootOperationUID;
    }

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

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
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

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public static TransformationProgress constructByDates(String sourceName, Date startDate, Date endDate)
            throws InstantiationException, IllegalAccessException {
        TransformationProgress progress = new TransformationProgress();
        progress.setSourceName(sourceName);
        progress.setStartDate(startDate);
        progress.setEndDate(endDate);

        progress.setRootOperationUID(UUID.randomUUID().toString().toUpperCase());
        progress.setStatus(TransformationProgressStatus.NEW);

        return progress;
    }

    @Override
    public String toString() {
        return String.format("TransformationProgress %s [%s]", sourceName, rootOperationUID);
    }
}
