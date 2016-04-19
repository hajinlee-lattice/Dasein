package com.latticeengines.domain.exposed.propdata.manage;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

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

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.ParamDef;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.propdata.publication.PublicationDestination;

@Entity
@Access(AccessType.FIELD)
@Table(name = "PublicationProgress")
@FilterDef(name = "hdfsPodFilter", parameters = {@ParamDef(name="hdfsPod", type="string")})
@Filter(name = "hdfsPodFilter", condition = "HdfsPod = :hdfsPod")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PublicationProgress implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", nullable = false)
    private Long pid;

    @ManyToOne(cascade = { CascadeType.MERGE, CascadeType.REMOVE })
    @JoinColumn(name = "FK_Publication", nullable = false)
    private Publication publication;

    @Column(name = "SourceVersion", nullable = false, length = 50)
    private String sourceVersion;

    @Column(name = "Destination", nullable = false, length = 1000)
    private String destinationString;

    @Enumerated(EnumType.STRING)
    @Column(name = "Status", nullable = false, length = 20)
    private Status status;

    @Column(name = "CreateTime", nullable = false)
    protected Date createTime = new Date();

    @Column(name = "LatestStatusUpdate", nullable = false)
    protected Date latestStatusUpdate;

    @Column(name = "CreatedBy", nullable = false)
    private String createdBy;

    @Column(name = "ApplicationId")
    private String applicationId;

    @Column(name = "Progress")
    private Float progress;

    @Column(name = "ErrorMessage", length = 1000)
    private String errorMessage;

    @Column(name = "Retries")
    private Integer retries;

    @Column(name = "HdfsPod")
    private String hdfsPod;

    @Override
    @JsonProperty("PID")
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonProperty("PID")
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @JsonIgnore
    public Publication getPublication() {
        return publication;
    }

    @JsonIgnore
    public void setPublication(Publication publication) {
        this.publication = publication;
    }

    @JsonProperty("PublicationName")
    private String getPublicationName() {
        return publication == null ? null: publication.getPublicationName();
    }

    @JsonProperty("SourceVersion")
    public String getSourceVersion() {
        return sourceVersion;
    }

    @JsonProperty("SourceVersion")
    public void setSourceVersion(String sourceVersion) {
        this.sourceVersion = sourceVersion;
    }

    @JsonIgnore
    private String getDestinationString() {
        return destinationString;
    }

    @JsonIgnore
    private void setDestinationString(String destinationString) {
        this.destinationString = destinationString;
    }

    @JsonProperty("Status")
    public Status getStatus() {
        return status;
    }

    @JsonProperty("Status")
    public void setStatus(Status status) {
        this.status = status;
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

    @JsonProperty("CreatedBy")
    public String getCreatedBy() {
        return createdBy;
    }

    @JsonProperty("CreatedBy")
    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    @JsonProperty("Progress")
    public Float getProgress() {
        return progress;
    }

    @JsonProperty("Progress")
    public void setProgress(Float progress) {
        this.progress = progress;
    }

    @JsonProperty("Retries")
    public Integer getRetries() {
        return retries;
    }

    @JsonProperty("Retries")
    public void setRetries(Integer retries) {
        this.retries = retries;
    }

    @JsonProperty("ErrorMessage")
    public String getErrorMessage() {
        return errorMessage;
    }

    @JsonProperty("ErrorMessage")
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @JsonProperty("ApplicationId")
    public String getApplicationId() {
        return applicationId;
    }

    @JsonProperty("ApplicationId")
    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
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

    @JsonProperty("Destination")
    public PublicationDestination getDestination() {
        return JsonUtils.deserialize(destinationString, PublicationDestination.class);
    }

    @JsonProperty("Destination")
    public void setDestination(PublicationDestination destination) {
        destinationString = JsonUtils.serialize(destination);
    }

    @JsonProperty("HdfsPod")
    public String getHdfsPod() {
        return hdfsPod;
    }

    @JsonProperty("HdfsPod")
    public void setHdfsPod(String hdfsPod) {
        this.hdfsPod = hdfsPod;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public enum Status {
        FAILED,
        NEW,
        PUBLISHING,
        FINISHED;

        public static Set<Status> terminalStatus() {
            return new HashSet<>(Arrays.asList(FAILED, FINISHED));
        }
    }

}
