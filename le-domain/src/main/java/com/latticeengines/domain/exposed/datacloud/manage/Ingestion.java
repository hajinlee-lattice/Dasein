package com.latticeengines.domain.exposed.datacloud.manage;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.List;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.ingestion.ProviderConfiguration;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

/**
 * Keep doc
 * https://confluence.lattice-engines.com/display/ENG/DataCloud+Engine+Architecture#DataCloudEngineArchitecture-Ingestion
 * up to date if there is any new change
 */
@Entity
@Access(AccessType.FIELD)
@Table(name = "Ingestion")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Ingestion implements HasPid, Serializable {

    private static final long serialVersionUID = -3466377418296501351L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    // Unique ingestion job name
    @Column(name = "IngestionName", unique = true, nullable = false, length = 100)
    private String ingestionName;

    @Column(name = "Config", nullable = false, length = 1000)
    private String config;

    // Only effective when SchedularEnabled is 1
    // If provided with cron expression, ingestion job is only triggered by
    // quartz when cron condition is satisfied
    // If not provided, ingestion job is always triggered by quartz
    @Column(name = "CronExpression", length = 100)
    private String cronExpression;

    // 1: Ingestion job is enabled to be triggered
    // 0: Ingestion job is disabled
    @Column(name = "SchedularEnabled", nullable = false)
    private Boolean schedularEnabled;

    @Deprecated
    // Original definition: If a failed job needs to retry, waiting duration in
    // milliseconds before starting next retry
    @Column(name = "NewJobRetryInterval", nullable = false)
    private Long newJobRetryInterval;

    // If a job is failed, maximum number of times of retry
    @Column(name = "NewJobMaxRetry", nullable = false)
    private int newJobMaxRetry;

    // Type of data provider
    @Enumerated(EnumType.STRING)
    @Column(name = "IngestionType", nullable = false, length = 100)
    private IngestionType ingestionType;

    // Latest trigger time of the ingestion
    @Column(name = "LatestTriggerTime")
    private Date latestTriggerTime;

    @Transient
    private ProviderConfiguration providerConfiguration;

    @OneToMany(fetch = FetchType.LAZY, mappedBy = "ingestion")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @Fetch(FetchMode.SUBSELECT)
    private List<IngestionProgress> progresses;

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
        return ingestionName;
    }

    @JsonProperty("IngestionName")
    public void setIngestionName(String ingestionName) {
        this.ingestionName = ingestionName;
    }

    @JsonIgnore
    private String getConfig() {
        return config;
    }

    @JsonIgnore
    public void setConfig(String config) {
        this.config = config;
    }

    @JsonProperty("CronExpression")
    public String getCronExpression() {
        return cronExpression;
    }

    @JsonProperty("CronExpression")
    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    @JsonProperty("SchedularEnabled")
    public Boolean isSchedularEnabled() {
        return schedularEnabled;
    }

    @JsonProperty("SchedularEnabled")
    public void setSchedularEnabled(Boolean schedularEnabled) {
        this.schedularEnabled = schedularEnabled;
    }

    @JsonProperty("NewJobRetryInterval")
    public Long getNewJobRetryInterval() {
        return newJobRetryInterval;
    }

    @JsonProperty("NewJobRetryInterval")
    public void setNewJobRetryInterval(Long newJobRetryInterval) {
        this.newJobRetryInterval = newJobRetryInterval;
    }

    @JsonProperty("NewJobMaxRetry")
    public int getNewJobMaxRetry() {
        return newJobMaxRetry;
    }

    @JsonProperty("NewJobMaxRetry")
    public void setNewJobMaxRetry(int newJobMaxRetry) {
        this.newJobMaxRetry = newJobMaxRetry;
    }

    @JsonProperty("IngestionType")
    public IngestionType getIngestionType() {
        return this.ingestionType;
    }

    @JsonProperty("IngestionType")
    public void setIngestionType(IngestionType ingestionType) {
        this.ingestionType = ingestionType;
    }

    @JsonProperty("LatestTriggerTime")
    public Date getLatestTriggerTime() {
        return latestTriggerTime;
    }

    @JsonProperty("LatestTriggerTime")
    public void setLatestTriggerTime(Date latestTriggerTime) {
        this.latestTriggerTime = latestTriggerTime;
    }

    @JsonIgnore
    public ProviderConfiguration getProviderConfiguration() {
        if (this.providerConfiguration == null) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                providerConfiguration = mapper.readValue(getConfig(), ProviderConfiguration.class);
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_25016, e, new String[] { ingestionName });
            }
        }
        return this.providerConfiguration;
    }

    @JsonIgnore
    public void setProviderConfiguration(ProviderConfiguration providerConfiguration) {
        this.providerConfiguration = providerConfiguration;
        ObjectMapper mapper = new ObjectMapper();
        try {
            setConfig(mapper.writeValueAsString(providerConfiguration));
        } catch (JsonProcessingException e) {
            throw new LedpException(LedpCode.LEDP_25016, e, new String[] { ingestionName });
        }
    }

    @JsonIgnore
    public List<IngestionProgress> getProgresses() {
        return progresses;
    }

    @JsonIgnore
    public void setProgresses(List<IngestionProgress> progresses) {
        this.progresses = progresses;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public enum IngestionType {
        SFTP, // generic
        SQL_TO_CSVGZ, // deprecated
        API, // generic
        SQL_TO_SOURCE, // deprecated
        S3, // only for datacloud collector
        BW_RAW, // only for datacloud collector
        PATCH_BOOK // only for datacloud patch book;
    }
}
