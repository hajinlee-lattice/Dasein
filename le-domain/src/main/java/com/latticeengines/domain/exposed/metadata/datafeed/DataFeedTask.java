package com.latticeengines.domain.exposed.metadata.datafeed;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.Table;

@Entity
@javax.persistence.Table(name = "DATAFEED_TASK", //
        indexes = { @Index(name = "IX_UNIQUE_ID", columnList = "UNIQUE_ID") }, //
        uniqueConstraints = @UniqueConstraint(columnNames = { "SOURCE", "FEED_TYPE", "`FK_FEED_ID`" }))
public class DataFeedTask implements HasPid, Serializable {

    private static final long serialVersionUID = -6740417234916797093L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonIgnore
    @ManyToOne
    @JoinColumn(name = "`FK_FEED_ID`", nullable = false)
    private DataFeed dataFeed;

    @Column(name = "UNIQUE_ID", unique = true, nullable = false)
    @JsonProperty("unique_id")
    private String uniqueId;

    @Column(name = "SOURCE", nullable = false)
    @JsonProperty("source")
    private String source;

    @Column(name = "ENTITY", nullable = false)
    @JsonProperty("entity")
    private String entity;

    @Column(name = "SOURCE_CONFIG", nullable = false, length = 1000)
    @JsonProperty("source_config")
    private String sourceConfig;

    @Column(name = "FEED_TYPE")
    @JsonProperty("feed_type")
    private String feedType;

    @JsonProperty("import_template")
    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JoinColumn(name = "FK_TEMPLATE_ID")
    private Table importTemplate;

    @JsonProperty("import_data")
    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JoinColumn(name = "`FK_DATA_ID`")
    private Table importData;

    @Column(name = "ACTIVE_JOB", nullable = false)
    @JsonProperty("active_job")
    private String activeJob;

    @Column(name = "STATUS", nullable = false)
    @JsonProperty("status")
    @Enumerated(EnumType.STRING)
    private Status status;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "START_TIME", nullable = false)
    @JsonProperty("start_time")
    private Date startTime;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "LAST_IMPORTED", nullable = false)
    @JsonProperty("last_imported")
    private Date lastImported;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "LAST_UPDATED", nullable = false)
    @JsonProperty("last_updated")
    private Date lastUpdated;

    @Column(name = "TEMPLATE_DISPLAY_NAME")
    @JsonProperty("template_display_name")
    private String templateDisplayName;

    @Column(name = "SUBTYPE")
    @JsonProperty("subtype")
    @Enumerated(EnumType.STRING)
    private SubType subType;

    @Column(name = "S3_IMPORT_STATUS", length = 30)
    @JsonProperty("s3_import_status")
    @Enumerated(EnumType.STRING)
    private S3ImportStatus s3ImportStatus = S3ImportStatus.Active;

    @JsonIgnore
    @Transient
    private List<DataFeedTaskTable> tables = new ArrayList<>();

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public DataFeed getDataFeed() {
        return dataFeed;
    }

    public void setDataFeed(DataFeed dataFeed) {
        this.dataFeed = dataFeed;
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public void setUniqueId(String uniqueId) {
        this.uniqueId = uniqueId;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getFeedType() {
        return feedType;
    }

    public void setFeedType(String feedType) {
        this.feedType = feedType;
    }

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    public Table getImportTemplate() {
        return importTemplate;
    }

    public void setImportTemplate(Table importTemplate) {
        this.importTemplate = importTemplate;
    }

    public Table getImportData() {
        return importData;
    }

    public void setImportData(Table importData) {
        this.importData = importData;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getActiveJob() {
        return activeJob;
    }

    public void setActiveJob(String activeJob) {
        this.activeJob = activeJob;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getLastImported() {
        return lastImported;
    }

    public void setLastImported(Date lastImported) {
        this.lastImported = lastImported;
    }

    public String getSourceConfig() {
        return sourceConfig;
    }

    public void setSourceConfig(String sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    public String getTemplateDisplayName() {
        return templateDisplayName;
    }

    public void setTemplateDisplayName(String templateDisplayName) {
        this.templateDisplayName = templateDisplayName;
    }

    public SubType getSubType() {
        return subType;
    }

    public void setSubType(SubType subType) {
        this.subType = subType;
    }

    public void setSubType(String subType) {
        if (StringUtils.isNotBlank(subType)) {
            try {
                SubType dftSubType = SubType.valueOf(subType);
                this.subType = dftSubType;
            } catch (IllegalArgumentException e) {
                this.subType = null;
            }
        }
    }

    public S3ImportStatus getS3ImportStatus() {
        return s3ImportStatus;
    }

    public void setS3ImportStatus(S3ImportStatus s3ImportStatus) {
        this.s3ImportStatus = s3ImportStatus;
    }

    public List<DataFeedTaskTable> getTables() {
        return tables;
    }

    public void setTables(List<DataFeedTaskTable> tables) {
        this.tables = tables;
    }

    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public enum Status {
        Initiating, //
        Active, //
        Updated, //
        Deleting
    }

    public enum SubType {
        Bundle, Hierarchy, Lead
    }

    public enum S3ImportStatus {
        Active,
        Pause
    }
}
