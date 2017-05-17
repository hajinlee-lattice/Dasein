package com.latticeengines.domain.exposed.cdl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.UniqueConstraint;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.Table;

@Entity
@javax.persistence.Table(name = "DATAFEED_TASK", uniqueConstraints = @UniqueConstraint(columnNames = { "FEED_ID",
        "SOURCE", "ENTITY" }))
public class DataFeedTask implements HasPid, Serializable {

    private static final long serialVersionUID = -6740417234916797093L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonIgnore
    @ManyToOne
    @JoinColumn(name = "FK_FEED_ID", nullable = false)
    private DataFeed dataFeed;

    @JsonIgnore
    @Column(name = "FEED_ID", nullable = false)
    private Long feedId;

    @Column(name = "SOURCE", nullable = false)
    @JsonProperty("source")
    private String source;

    @Column(name = "ENTITY", nullable = false)
    @JsonProperty("entity")
    private String entity;

    @Column(name = "SOURCE_CONFIG", nullable = false)
    @JsonProperty("sourceConfig")
    private String sourceConfig;

    @JsonIgnore
    @OneToOne
    @JoinColumn(name = "FK_TEMPLATE_ID", nullable = false)
    private Table importTemplate;

    @JsonIgnore
    @OneToOne
    @JoinColumn(name = "FK_DATA_ID", nullable = false)
    private Table importData;

    @Column(name = "STAGING_DIR", nullable = false)
    @JsonIgnore
    private String stagingDir;

    @Column(name = "ACTIVE_JOB", nullable = false)
    @JsonIgnore
    private Long activeJob;

    @Column(name = "STATUS", nullable = false)
    @JsonProperty("status")
    private Status status;

    @Column(name = "LAST_CONSOLIDATED", nullable = false)
    @JsonProperty("lastConsolidated")
    private Long lastConsolidated;

    @Column(name = "LAST_IMPORTED", nullable = false)
    @JsonProperty("lastImported")
    private Long lastImported;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public Long getFeedId() {
        return feedId;
    }

    public void setFeedId(Long feedId) {
        this.feedId = feedId;
    }

    public DataFeed getFeed() {
        return dataFeed;
    }

    public void setFeed(DataFeed feed) {
        this.feedId = feed.getPid();
        this.dataFeed = feed;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
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

    public String getStagingDir() {
        return stagingDir;
    }

    public void setStagingDir(String stagingDir) {
        this.stagingDir = stagingDir;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public long getActiveJob() {
        return activeJob;
    }

    public void setActiveJob(long activeJob) {
        this.activeJob = activeJob;
    }

    public long getLastConsolidated() {
        return lastConsolidated;
    }

    public void setLastConsolidated(Long lastConsolidated) {
        this.lastConsolidated = lastConsolidated;
    }

    public long getLastImported() {
        return lastImported;
    }

    public void setLastImported(Long lastImported) {
        this.lastImported = lastImported;
    }

    public static enum Status {
        Initing("inited"), //
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
