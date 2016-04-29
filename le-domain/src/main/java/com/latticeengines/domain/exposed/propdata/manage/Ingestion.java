package com.latticeengines.domain.exposed.propdata.manage;

import java.io.IOException;
import java.io.Serializable;
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
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.ingestion.Protocol;

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

    @Column(name = "IngestionName", unique = true, nullable = false, length = 100)
    private String ingestionName;

    @Column(name = "Source", nullable = false, length = 1000)
    private String source;

    @Column(name = "CronExpression", length = 100)
    private String cronExpression;

    @Column(name = "SchedularEnabled", nullable = false)
    private Boolean schedularEnabled;

    @Column(name = "NewJobRetryInterval", nullable = false)
    private Long newJobRetryInterval;

    @Column(name = "NewJobMaxRetry", nullable = false)
    private int newJobMaxRetry;

    @Enumerated(EnumType.STRING)
    @Column(name = "IngestionType", nullable = false, length = 100)
    private IngestionType ingestionType;

    @Enumerated(EnumType.STRING)
    @Column(name = "IngestionCriteria", nullable = false, length = 100)
    private IngestionCriteria ingestionCriteria;

    @Transient
    private Protocol protocol;

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
    private String getSource() {
        return source;
    }

    @JsonIgnore
    private void setSource(String source) {
        this.source = source;
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

    @JsonProperty("IngestionCriteria")
    public IngestionCriteria getIngestionCriteria() {
        return ingestionCriteria;
    }

    @JsonProperty("IngestionCriteria")
    public void setIngestionCriteria(IngestionCriteria ingestionCriteria) {
        this.ingestionCriteria = ingestionCriteria;
    }

    @JsonIgnore
    public Protocol getProtocol() {
        if (this.protocol == null) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                protocol = mapper.readValue(getSource(), Protocol.class);
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_25015, e, new String[] { ingestionName });
            }
        }
        return this.protocol;
    }

    @JsonIgnore
    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
        ObjectMapper mapper = new ObjectMapper();
        try {
            setSource(mapper.writeValueAsString(protocol));
        } catch (JsonProcessingException e) {
            throw new LedpException(LedpCode.LEDP_25015, e, new String[] { ingestionName });
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
        SFTP_TO_HDFS;
    }

    public enum IngestionCriteria {
        ANY_MISSING_FILE;
    }
}
