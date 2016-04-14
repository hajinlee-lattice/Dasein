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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.propdata.publication.PublicationDestination;

@Entity
@Access(AccessType.FIELD)
@Table(name = "PublicationProgress")
@JsonInclude(JsonInclude.Include.NON_NULL)
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
    private String appId;

    @Column(name = "Progress")
    private Float progress;

    @Column(name = "ErrorMessage", length = 1000)
    private String errorMessage;

    @Column(name = "Retries")
    private Integer retries;

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
        return publication.getPublicationName();
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
    public String getDestinationString() {
        return destinationString;
    }

    @JsonIgnore
    public void setDestinationString(String destinationString) {
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

    @JsonIgnore
    private String getAppId() {
        return appId;
    }

    @JsonIgnore
    private void setAppId(String appId) {
        this.appId = appId;
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
    public ApplicationId getApplicationId() {
        return appId == null ? null : ConverterUtils.toApplicationId(appId);
    }

    @JsonProperty("ApplicationId")
    public void setApplicationId(ApplicationId applicationId) {
        this.appId = applicationId.toString();
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
