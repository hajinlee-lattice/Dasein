package com.latticeengines.domain.exposed.cdl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Transient;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "DATAFEED")
public class DataFeed implements HasName, HasPid, HasTenantId, Serializable {

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
    private DataCollection collection;

    @Column(name = "NAME", nullable = false)
    @JsonProperty("name")
    private String name;

    @Column(name = "STATUS", nullable = false)
    @JsonProperty("status")
    private Status status;

    @Column(name = "ACTIVE_EXECUTION", nullable = false)
    @JsonProperty("activeExecution")
    private Long activeExecution;

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "dataFeed")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("executions")
    private List<DataFeedExecution> executions;

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER, mappedBy = "dataFeed")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("tasks")
    private List<DataFeedTask> tasks;

    @Transient
    @JsonIgnore
    private Map<String, Map<String, DataFeedTask>> taskMap = new HashMap<>();

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

    public List<DataFeedExecution> getExecutions() {
        return executions;
    }

    public void addExeuction(DataFeedExecution exec) {
        exec.setFeed(this);
        executions.add(exec);
    }

    public void setExecutions(List<DataFeedExecution> executions) {
        this.executions = executions;
    }

    public Long getActiveExecution() {
        return activeExecution;
    }

    public void setActiveExecution(Long execution) {
        // Set it to null when executioin is finished.
        activeExecution = execution;
    }

    public List<DataFeedTask> getTasks() {
        return tasks;
    }

    public DataFeedTask getTask(String entity, String src) {
        Map<String, DataFeedTask> taskSrcMap = taskMap.get(entity);
        return ((tasks == null) ? null : taskSrcMap.get(src));
    }

    public void addTask(DataFeedTask task) {
        Map<String, DataFeedTask> taskSrcMap = taskMap.get(task.getEntity());
        if (tasks == null) {
            taskSrcMap = new HashMap<String, DataFeedTask>();
            taskMap.put(task.getEntity(), taskSrcMap);
        }
        taskSrcMap.put(task.getSource(), task);
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

    public static enum Status {
        Initing("initing"), //
        InitialLoad("initialLoad"), //
        Active("active"), //
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
