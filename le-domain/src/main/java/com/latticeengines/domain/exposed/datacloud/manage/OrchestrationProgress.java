package com.latticeengines.domain.exposed.datacloud.manage;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Access;
import javax.persistence.AccessType;
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
import javax.persistence.Transient;

import org.apache.commons.lang3.tuple.Pair;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.ParamDef;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngine;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "OrchestrationProgress")
@FilterDef(name = "hdfsPodFilter", parameters = { @ParamDef(name = "hdfsPod", type = "string") })
@Filter(name = "hdfsPodFilter", condition = "HdfsPod = :hdfsPod")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrchestrationProgress implements HasPid, Serializable {

    private static final long serialVersionUID = 4924847774951669528L;
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne
    @JoinColumn(name = "Orchestration", nullable = false)
    private Orchestration orchestration;

    @Column(name = "Version", length = 50)
    private String version;

    @Column(name = "HdfsPod", nullable = false, length = 100)
    private String hdfsPod;

    @Enumerated(EnumType.STRING)
    @Column(name = "Status", nullable = false, length = 20)
    private ProgressStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "CurrentStage", length = 20)
    private DataCloudEngine currentStage;

    @Transient
    private Pair<DataCloudEngine, String> currentStep;

    @Column(name = "StartTime", nullable = false)
    private Date startTime;

    @Column(name = "LatestUpdateTime", nullable = false)
    private Date latestUpdateTime;

    @Column(name = "ApplicationId", length = 50)
    private String applicationId;

    @Column(name = "TriggeredBy", nullable = false, length = 50)
    private String triggeredBy;

    @Column(name = "Retries", nullable = false)
    private int retries;

    @Column(name = "Message", length = 1000)
    private String message;

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

    @JsonProperty("Version")
    public String getVersion() {
        return version;
    }

    @JsonProperty("Version")
    public void setVersion(String version) {
        this.version = version;
    }

    @JsonProperty("HdfsPod")
    public String getHdfsPod() {
        return hdfsPod;
    }

    @JsonProperty("HdfsPod")
    public void setHdfsPod(String hdfsPod) {
        this.hdfsPod = hdfsPod;
    }

    @JsonProperty("Status")
    public ProgressStatus getStatus() {
        return status;
    }

    @JsonProperty("Status")
    public void setStatus(ProgressStatus status) {
        this.status = status;
    }

    @JsonProperty("CurrentStage")
    public DataCloudEngine getCurrentStage() {
        return currentStage;
    }

    @JsonProperty("CurrentStage")
    public void setCurrentStage(DataCloudEngine currentStage) {
        this.currentStage = currentStage;
    }

    @JsonProperty("StartTime")
    public Date getStartTime() {
        return startTime;
    }

    @JsonProperty("StartTime")
    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    @JsonProperty("LatestUpdateTime")
    public Date getLatestUpdateTime() {
        return latestUpdateTime;
    }

    @JsonProperty("LatestUpdateTime")
    public void setLatestUpdateTime(Date latestUpdateTime) {
        this.latestUpdateTime = latestUpdateTime;
    }

    @JsonProperty("ApplicationId")
    public String getApplicationId() {
        return applicationId;
    }

    @JsonProperty("ApplicationId")
    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    @JsonProperty("TriggeredBy")
    public String getTriggeredBy() {
        return triggeredBy;
    }

    @JsonProperty("TriggeredBy")
    public void setTriggeredBy(String triggeredBy) {
        this.triggeredBy = triggeredBy;
    }

    @JsonProperty("Retries")
    public int getRetries() {
        return retries;
    }

    @JsonProperty("Retries")
    public void setRetries(int retries) {
        this.retries = retries;
    }

    @JsonProperty("Message")
    public String getMessage() {
        return message;
    }

    @JsonProperty("Message")
    public void setMessage(String message) {
        this.message = message;
    }

    @JsonIgnore
    public Pair<DataCloudEngine, String> getCurrentStep() {
        return currentStep;
    }

    @JsonIgnore
    public void setCurrentStep(Pair<DataCloudEngine, String> currentStep) {
        this.currentStep = currentStep;
    }

    @JsonIgnore
    public Orchestration getOrchestration() {
        return orchestration;
    }

    @JsonIgnore
    public void setOrchestration(Orchestration orchestration) {
        this.orchestration = orchestration;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
