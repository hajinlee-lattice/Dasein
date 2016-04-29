package com.latticeengines.domain.exposed.propdata.manage;

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

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.ParamDef;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "IngestionProgress")
@FilterDef(name = "hdfsPodFilter", parameters = { @ParamDef(name = "hdfsPod", type = "string") })
@Filter(name = "hdfsPodFilter", condition = "HdfsPod = :hdfsPod")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class IngestionProgress implements HasPid, Serializable {

    private static final long serialVersionUID = -1412771506655952055L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne
    @JoinColumn(name = "IngestionId", nullable = false)
    private Ingestion ingestion;

    @Column(name = "Source", nullable = false, length = 1000)
    private String source;

    @Column(name = "Destination", nullable = false, length = 1000)
    private String destination;

    @Column(name = "HdfsPod", nullable = false, length = 100)
    private String hdfsPod;

    @Enumerated(EnumType.STRING)
    @Column(name = "Status", nullable = false, length = 20)
    private ProgressStatus status;

    @Column(name = "StartTime")
    private Date startTime;

    @Column(name = "LastestStatusUpdateTime", nullable = false)
    private Date lastestStatusUpdateTime;

    @Column(name = "ApplicationId", length = 50)
    private String applicationId;

    @Column(name = "TriggeredBy", nullable = false, length = 50)
    private String triggeredBy;

    @Column(name = "Size")
    private Long size;

    @Column(name = "Retries", nullable = false)
    private int retries;

    @Column(name = "ErrorMessage", length = 1000)
    private String errorMessage;

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

    @JsonProperty("IngestionName")
    public String getIngestionName() {
        return getIngestion() == null ? null : getIngestion().getIngestionName();
    }

    @JsonIgnore
    public Ingestion getIngestion() {
        return ingestion;
    }

    @JsonIgnore
    public void setIngestion(Ingestion ingestion) {
        this.ingestion = ingestion;
    }

    @JsonProperty("Source")
    public String getSource() {
        return source;
    }

    @JsonProperty("Source")
    public void setSource(String source) {
        this.source = source;
    }

    @JsonProperty("Destination")
    public String getDestination() {
        return destination;
    }

    @JsonProperty("Destination")
    public void setDestination(String destination) {
        this.destination = destination;
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

    @JsonProperty("StartTime")
    public Date getStartTime() {
        return startTime;
    }

    @JsonProperty("StartTime")
    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    @JsonProperty("LastestStatusUpdateTime")
    public Date getLastestStatusUpdateTime() {
        return lastestStatusUpdateTime;
    }

    @JsonProperty("LastestStatusUpdateTime")
    public void setLastestStatusUpdateTime(Date lastestStatusUpdateTime) {
        this.lastestStatusUpdateTime = lastestStatusUpdateTime;
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

    @JsonProperty("Size")
    public Long getSize() {
        return size;
    }

    @JsonProperty("Size")
    public void setSize(Long size) {
        this.size = size;
    }

    @JsonProperty("Retries")
    public int getRetries() {
        return retries;
    }

    @JsonProperty("Retries")
    public void setRetries(int retries) {
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

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
