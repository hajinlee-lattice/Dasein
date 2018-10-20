package com.latticeengines.domain.exposed.datacloud.manage;

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
import javax.persistence.Index;
import javax.persistence.Table;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.ParamDef;

import com.fasterxml.jackson.annotation.JsonIgnore;

@Entity
@Access(AccessType.FIELD)
@Table(name = "TransformationProgress", indexes = {
        @Index(name = "IX_SOURCE", columnList = "SourceName"), //
        @Index(name = "IX_PIPELINE_VERSION", columnList = "SourceName,PipelineName,Version") //
})
@FilterDef(name = "hdfsPodFilter", parameters = { @ParamDef(name = "hdfsPod", type = "string") })
@Filter(name = "hdfsPodFilter", condition = "HdfsPod = :hdfsPod")
public class TransformationProgress implements Progress {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ProgressID", unique = true, nullable = false)
    protected Long pid;

    @Column(name = "SourceName", nullable = false)
    protected String sourceName;

    @Column(name = "PipelineName")
    protected String pipelineName;

    @Column(name = "StartDate")
    protected Date startDate;

    @Column(name = "EndDate")
    protected Date endDate;

    @Enumerated(EnumType.STRING)
    @Column(name = "Status")
    protected ProgressStatus status;
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
    @Column(name = "HdfsPod", nullable = false, length = 100)
    private String hdfsPod;
    @Column(name = "LatestStatusUpdate")
    private Date latestStatusUpdate;
    @Column(name = "RootOperationUID", unique = true, nullable = false)
    private String rootOperationUID;
    @Column(name = "BaseSourceVersions")
    private String baseSourceVersions;
    @Column(name = "YarnAppId")
    private String yarnAppId;

    public static TransformationProgress constructByDates(String sourceName, Date startDate,
            Date endDate) throws InstantiationException, IllegalAccessException {
        TransformationProgress progress = new TransformationProgress();
        progress.setSourceName(sourceName);
        progress.setStartDate(startDate);
        progress.setEndDate(endDate);

        progress.setRootOperationUID(UUID.randomUUID().toString().toUpperCase());
        progress.setStatus(ProgressStatus.NEW);

        return progress;
    }

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

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
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

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getBaseSourceVersions() {
        return baseSourceVersions;
    }

    public void setBaseSourceVersions(String baseSourceVersions) {
        this.baseSourceVersions = baseSourceVersions;
    }

    public String getYarnAppId() {
        return yarnAppId;
    }

    public void setYarnAppId(String yarnAppId) {
        this.yarnAppId = yarnAppId;
    }

    public String getHdfsPod() {
        return hdfsPod;
    }

    public void setHdfsPod(String hdfsPod) {
        this.hdfsPod = hdfsPod;
    }

    @Override
    public String toString() {
        return String.format("TransformationProgress %s [%s]", sourceName, rootOperationUID);
    }

    @Override
    @JsonIgnore
    public ProgressStatus getStatusBeforeFailed() {
        throw new UnsupportedOperationException("Deprecated operation.");
    }

    @Override
    @JsonIgnore
    public void setStatusBeforeFailed(ProgressStatus status) {
        throw new UnsupportedOperationException("Deprecated operation.");
    }

}
