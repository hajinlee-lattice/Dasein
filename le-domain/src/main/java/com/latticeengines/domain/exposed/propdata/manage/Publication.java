package com.latticeengines.domain.exposed.propdata.manage;

import java.util.List;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.CascadeType;
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

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.propdata.publication.PublicationConfiguration;


@Entity
@Access(AccessType.FIELD)
@Table(name = "Publication")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Publication implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", nullable = true)
    private Long pid;

    @Column(name = "PublicationName", unique = true, nullable = false, length = 100)
    private String publicationName;

    @Column(name = "SourceName", nullable = false, length = 100)
    private String sourceName;

    @Column(name = "DestinationConfig", nullable = false, length = 1000)
    private String destConfigString;

    @Enumerated(EnumType.STRING)
    @Column(name = "PublicationType", nullable = false, length = 20)
    private PublicationType publicationType;

    @Column(name = "CronExpression", length=20)
    protected String cronExpression;

    @Column(name = "SchedularEnabled")
    protected Boolean schedularEnabled;

    @Column(name = "NewJobRetryInterval")
    protected Long newJobRetryInterval;

    @Column(name = "NewJobMaxRetry")
    protected Integer newJobMaxRetry;

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "publication")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @Fetch(FetchMode.SUBSELECT)
    private List<PublicationProgress> progresses;

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

    @JsonProperty("PublicationName")
    public String getPublicationName() {
        return publicationName;
    }

    @JsonProperty("PublicationName")
    public void setPublicationName(String publicationName) {
        this.publicationName = publicationName;
    }

    @JsonProperty("SourceName")
    public String getSourceName() {
        return sourceName;
    }

    @JsonProperty("SourceName")
    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    @JsonIgnore
    private String getDestConfigString() {
        return destConfigString;
    }

    @JsonIgnore
    private void setDestConfigString(String destConfigString) {
        this.destConfigString = destConfigString;
    }

    @JsonProperty("PublicationType")
    public PublicationType getPublicationType() {
        return publicationType;
    }

    @JsonProperty("PublicationType")
    public void setPublicationType(PublicationType publicationType) {
        this.publicationType = publicationType;
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
    public Integer getNewJobMaxRetry() {
        return newJobMaxRetry;
    }

    @JsonProperty("NewJobMaxRetry")
    public void setNewJobMaxRetry(Integer newJobMaxRetry) {
        this.newJobMaxRetry = newJobMaxRetry;
    }

    @JsonProperty("Progresses")
    public List<PublicationProgress> getProgresses() {
        return progresses;
    }

    @JsonProperty("Progresses")
    public void setProgresses(List<PublicationProgress> progresses) {
        this.progresses = progresses;
    }

    @JsonProperty("DestinationConfiguration")
    public PublicationConfiguration getDestinationConfiguration() {
        return JsonUtils.deserialize(destConfigString, PublicationConfiguration.class);
    }

    @JsonProperty("DestinationConfiguration")
    public void setDestinationConfiguration(PublicationConfiguration configuration) {
        this.destConfigString = JsonUtils.serialize(configuration);
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public enum PublicationType {
        SQL
    }

}
