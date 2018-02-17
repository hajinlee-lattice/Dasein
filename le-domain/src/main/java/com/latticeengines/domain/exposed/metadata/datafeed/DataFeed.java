package com.latticeengines.domain.exposed.metadata.datafeed;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "DATAFEED", //
        indexes = { @Index(name = "IX_FEED_NAME", columnList = "NAME") }, //
        uniqueConstraints = @UniqueConstraint(columnNames = { "TENANT_ID", "NAME" }))
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
    @JsonProperty("tenant")
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
    private String name;

    @Column(name = "STATUS", nullable = false)
    @JsonProperty("status")
    @Enumerated(EnumType.STRING)
    private Status status;

    @Column(name = "ACTIVE_EXECUTION", nullable = true)
    @JsonIgnore
    private Long activeExecutionId;

    @Transient
    @JsonProperty("active_execution")
    private DataFeedExecution activeExecution;

    @Column(name = "EARLIEST_TRANSACTION", nullable = true)
    @JsonProperty("earliest_transaction")
    private Integer earliestTransaction;

    @Column(name = "REBUILD_TRANSACTION", nullable = true)
    @JsonProperty("rebuildTransaction")
    private Boolean rebuildTransaction;

    @JsonIgnore
    @Transient
    private List<DataFeedExecution> executions = new ArrayList<>();

    @JsonProperty("tasks")
    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "dataFeed")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private List<DataFeedTask> tasks = new ArrayList<>();

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "LAST_PUBLISHED", nullable = true)
    @JsonProperty("last_published")
    private Date lastPublished;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "LAST_PROFILED", nullable = true)
    @JsonProperty("last_profiled")
    private Date lastProfiled;

    @Column(name = "DRAINING_STATUS", nullable = false)
    @Enumerated(EnumType.STRING)
    @JsonProperty("draining_status")
    private DrainingStatus drainingStatus = DrainingStatus.NONE;

    @Column(name = "AUTO_SCHEDULING", nullable = false)
    @JsonProperty("auto_scheduling")
    private boolean autoScheduling = false;

    @Column(name = "MAINTENANCE_MODE")
    @JsonProperty("maintenance_mode")
    private boolean maintenanceMode = false;

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

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;

        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
    }

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
        tasks.add(task);
    }

    public void setTasks(List<DataFeedTask> taskList) {
        for (DataFeedTask task : taskList) {
            addTask(task);
        }
    }

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
        this.activeExecution = activeExecution;
    }

    public Date getLastPublished() {
        return lastPublished;
    }

    public void setLastPublished(Date lastPublished) {
        this.lastPublished = lastPublished;
    }

    public Date getLastProfiled() {
        return lastProfiled;
    }

    public void setLastProfiled(Date lastProfiled) {
        this.lastProfiled = lastProfiled;
    }

    public DrainingStatus getDrainingStatus() {
        return drainingStatus;
    }

    public void setDrainingStatus(DrainingStatus drainingStatus) {
        this.drainingStatus = drainingStatus;
    }

    public boolean isAutoScheduling() {
        return autoScheduling;
    }

    public void setAutoScheduling(boolean autoScheduling) {
        this.autoScheduling = autoScheduling;
    }

    public boolean isMaintenanceMode() {
        return maintenanceMode;
    }

    public void setMaintenanceMode(boolean maintenanceMode) {
        this.maintenanceMode = maintenanceMode;
    }

    public Integer getEarliestTransaction() {
        return earliestTransaction;
    }

    public void setEarliestTransaction(Integer earliestTransaction) {
        this.earliestTransaction = earliestTransaction;
    }

    public Boolean getRebuildTransaction() {
        return rebuildTransaction;
    }

    public void setRebuildTransaction(Boolean rebuildTransaction) {
        this.rebuildTransaction = rebuildTransaction;
    }

    public enum Status {
        Initing("initing"), //
        Initialized("initialized") {
            @Override
            public Collection<DataFeedExecutionJobType> getAllowedJobTypes() {
                return Collections.singleton(DataFeedExecutionJobType.Import);
            }
        }, // import is ready to run
        InitialLoaded("initialLoaded") {
            @Override
            public Collection<DataFeedExecutionJobType> getAllowedJobTypes() {
                return Arrays.asList(DataFeedExecutionJobType.Import, //
                        DataFeedExecutionJobType.PA);
            }
        }, // initial import data loaded
        Active("active") {
            @Override
            public Collection<DataFeedExecutionJobType> getAllowedJobTypes() {
                return Arrays.asList(DataFeedExecutionJobType.Import, //
                        DataFeedExecutionJobType.CDLOperation, //
                        DataFeedExecutionJobType.PA);
            }

        }, // master table has formed and pushed to data store
        ProcessAnalyzing("processAnalyzing"), //
        Deleting("deleting");

        private final String name;
        private static Map<String, Status> nameMap;

        public Collection<DataFeedExecutionJobType> getAllowedJobTypes() {
            return Collections.emptySet();
        }

        static {
            nameMap = new HashMap<>();
            for (Status status : Status.values()) {
                nameMap.put(status.getName(), status);
            }
        }

        Status(String name) {
            this.name = name;
        }

        @JsonValue
        public String getName() {
            return StringUtils.capitalize(super.name());
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
