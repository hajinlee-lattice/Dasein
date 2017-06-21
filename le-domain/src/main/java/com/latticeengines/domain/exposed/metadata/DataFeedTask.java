package com.latticeengines.domain.exposed.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@javax.persistence.Table(name = "DATAFEED_TASK", uniqueConstraints = @UniqueConstraint(columnNames = { "SOURCE",
        "ENTITY", "FEED_TYPE", "FK_FEED_ID" }))
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

    @JsonIgnore
    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JoinColumn(name = "FK_DATA_ID")
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

    public List<DataFeedTaskTable> getTables() {
        return tables;
    }

    public void setTables(List<DataFeedTaskTable> tables) {
        this.tables = tables;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public static enum Status {
        Initiating("initiating"), //
        Active("active"), //
        Updated("updated"), //
        Deleting("deleting");

        private final String name;
        private static Map<String, Status> nameMap;

        static {
            nameMap = new HashMap<>();
            for (Status status : Status.values()) {
                nameMap.put(status.getName(), status);
            }
        }

        Status(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }

        public String toString() {
            return this.name;
        }

        public static Status fromName(String name) {
            if (name == null) {
                return null;
            }
            if (nameMap.containsKey(name)) {
                return nameMap.get(name);
            } else {
                throw new IllegalArgumentException("Cannot find a data feed status with name " + name);
            }
        }
    }
}
