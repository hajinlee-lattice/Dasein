package com.latticeengines.domain.exposed.metadata;

import java.io.Serializable;
import java.util.ArrayList;
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
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.apache.commons.lang.StringUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.Index;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "DATAFEED", uniqueConstraints = @UniqueConstraint(columnNames = { "TENANT_ID",
        "NAME" }))
@Filters({ @Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId") })
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataFeed implements HasName, HasPid, HasTenant, HasTenantId, Serializable {

    private static final long serialVersionUID = -6740417234916797093L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @JsonIgnore
    @ManyToOne
    @JoinColumn(name = "FK_COLLECTION_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private DataCollection dataCollection;

    @Column(name = "NAME", nullable = false)
    @JsonProperty("name")
    @Index(name = "IX_FEED_NAME")
    private String name;

    @Column(name = "STATUS", nullable = false)
    @JsonProperty("status")
    @Enumerated(EnumType.STRING)
    private Status status;

    @Column(name = "ACTIVE_EXECUTION", nullable = false)
    @JsonIgnore
    private Long activeExecutionId = 1L;

    @Transient
    @JsonProperty("active_execution")
    private DataFeedExecution activeExecution;

    @JsonIgnore
    @Transient
    private List<DataFeedExecution> executions = new ArrayList<>();

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "dataFeed")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("tasks")
    private List<DataFeedTask> tasks = new ArrayList<>();

    @Transient
    @JsonIgnore
    private Map<String, Map<String, Map<String, DataFeedTask>>> taskMap = new HashMap<>();

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    @JsonIgnore
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @JsonIgnore
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;

        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
    }

    @JsonIgnore
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public DataCollection getDataCollection() {
        return dataCollection;
    }

    public void setDataCollection(DataCollection dataCollection) {
        this.dataCollection = dataCollection;
    }

    public List<DataFeedExecution> getExecutions() {
        return executions;
    }

    public void addExeuction(DataFeedExecution exec) {
        executions.add(exec);
    }

    public void setExecutions(List<DataFeedExecution> executions) {
        this.executions = executions;
    }

    public Long getActiveExecutionId() {
        return activeExecutionId;
    }

    public void setActiveExecutionId(Long activeExecutionId) {
        // Set it to null when executioin is finished.
        this.activeExecutionId = activeExecutionId;
    }

    public List<DataFeedTask> getTasks() {
        return tasks;
    }

    public DataFeedTask getTask(String entity, String src, String feedType) {
        Map<String, Map<String, DataFeedTask>> taskSrcMap = taskMap.get(entity);
        if (taskSrcMap == null) {
            return null;
        }
        Map<String, DataFeedTask> taskTypeMap = taskSrcMap.get(src);
        return ((taskTypeMap == null) ? null : taskTypeMap.get(feedType));
    }

    public void addTask(DataFeedTask task) {
        Map<String, Map<String, DataFeedTask>> taskSrcMap = taskMap.get(task.getEntity());
        if (taskSrcMap == null) {
            Map<String, DataFeedTask> taskTypeMap = new HashMap<>();
            taskTypeMap.put(task.getFeedType(), task);
            taskSrcMap = new HashMap<>();
            taskSrcMap.put(task.getSource(), taskTypeMap);

            taskMap.put(task.getEntity(), taskSrcMap);
        } else {
            Map<String, DataFeedTask> taskTypeMap = taskSrcMap.get(task.getSource());
            if (taskTypeMap == null) {
                taskTypeMap = new HashMap<>();
                taskTypeMap.put(task.getFeedType(), task);
                taskSrcMap.put(task.getSource(), taskTypeMap);
            } else {
                taskTypeMap.put(task.getFeedType(), task);
            }
        }
        // taskSrcMap.put(task.getSource(), task);
        tasks.add(task);
    }

    public void setTasks(List<DataFeedTask> taskList) {
        for (DataFeedTask task : taskList) {
            addTask(task);
        }
    }

    // Missing updataTask, deleteTask

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public DataFeedExecution getActiveExecution() {
        return activeExecution;
    }

    public void setActiveExecution(DataFeedExecution activeExecution) {
        activeExecution.setDataFeed(this);
        this.activeExecution = activeExecution;
    }

    public enum Status {
        Initing("initing", false), // no template yet
        Initialized("initialized", false), // import is ready to run
        InitialLoaded("initialLoaded", true), // initial import data loaded
        InitialConsolidated("initialConsolidated", true), // initial data
                                                          // consolidated
        Active("active", true), // master table has formed and pushed to data
                                // store

        Consolidating("consolidating", false), //
        Finalizing("finalizing", false), //
        Deleting("deleting", false);

        private final String name;
        private boolean allowConsolidation;
        private static Map<String, Status> nameMap;

        static {
            nameMap = new HashMap<>();
            for (Status status : Status.values()) {
                nameMap.put(status.getName(), status);
            }
        }

        Status(String name, boolean allowConsolidation) {
            this.name = name;
            this.allowConsolidation = allowConsolidation;
        }

        @JsonValue
        public String getName() {
            return StringUtils.capitalize(super.name());
        }

        public String toString() {
            return this.name;
        }

        public boolean isAllowConsolidation() {
            return allowConsolidation;
        }

        public void setAllowConsolidation(boolean allowConsolidation) {
            this.allowConsolidation = allowConsolidation;
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
