package com.latticeengines.domain.exposed.metadata.datafeed;

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
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;

@Entity
@Table(name = "DATAFEED_EXECUTION")
public class DataFeedExecution implements HasPid, HasAuditingFields, Serializable {

    private static final long serialVersionUID = -6740417234916797093L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_FEED_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonIgnore
    private DataFeed dataFeed;

    @Column(name = "STATUS", nullable = false)
    @JsonProperty("status")
    @Enumerated(EnumType.STRING)
    private Status status;

    @JsonProperty("imports")
    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "execution")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private List<DataFeedImport> imports = new ArrayList<>();

    @JsonProperty("workflow_id")
    @Column(name = "WORKFLOW_ID", nullable = true)
    private Long workflowId;

    @JsonProperty("workflow_pid")
    @Column(name = "WORKFLOW_PID", nullable = true)
    private Long workflowPid;

    @Column(name = "JOB_TYPE", nullable = false)
    @JsonProperty("job_type")
    @Enumerated(EnumType.STRING)
    private DataFeedExecutionJobType jobType;

    @Column(name = "CREATED")
    @JsonProperty("created")
    @Temporal(TemporalType.TIMESTAMP)
    private Date created;

    @Column(name = "UPDATED")
    @JsonProperty("updated")
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated;

    @Column(name = "RETRY_COUNT")
    @JsonProperty("retry_count")
    private int retryCount = 0;

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

    public void setDataFeed(DataFeed feed) {
        this.dataFeed = feed;
    }

    public List<DataFeedImport> getImports() {
        return imports;
    }

    public void setImports(List<DataFeedImport> imports) {
        this.imports = imports;
    }

    public void addImport(DataFeedImport feedImport) {
        feedImport.setExecution(this);
        imports.add(feedImport);
    }

    public void addImports(List<DataFeedImport> imports) {
        imports.forEach(this::addImport);
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Long getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(Long workflowId) {
        this.workflowId = workflowId;
    }

    public Long getWorkflowPid() {
        return workflowPid;
    }

    public void setWorkflowPid(Long workflowPid) {
        this.workflowPid = workflowPid;
    }

    public DataFeedExecutionJobType getDataFeedExecutionJobType() {
        return this.jobType;
    }

    public void setDataFeedExecutionJobType(DataFeedExecutionJobType jobType) {
        this.jobType = jobType;
    }

    @Override
    public Date getCreated() {
        return created;
    }

    @Override
    public void setCreated(Date created) {
        this.created = created;
    }

    @Override
    public Date getUpdated() {
        return updated;
    }

    @Override
    public void setUpdated(Date updated) {
        this.updated = updated;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public enum Status {
        Inited("inited"), //
        Active("active"), //
        Started("started"), //
        Completed("completed"), //
        Failed("failed");

        private static Map<String, Status> nameMap;

        static {
            nameMap = new HashMap<>();
            for (Status status : Status.values()) {
                nameMap.put(status.getName(), status);
            }
        }

        private final String name;

        Status(String name) {
            this.name = name;
        }

        public static Status fromName(String name) {
            if (name == null) {
                return null;
            }
            if (nameMap.containsKey(name)) {
                return nameMap.get(name);
            } else {
                throw new IllegalArgumentException(
                        "Cannot find a data feed excution status with name " + name);
            }
        }

        public String getName() {
            return this.name;
        }

        public String toString() {
            return this.name;
        }
    }
}
