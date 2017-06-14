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

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@javax.persistence.Table(name = "DATAFEED_EXECUTION")
public class DataFeedExecution implements HasPid, Serializable {

    private static final long serialVersionUID = -6740417234916797093L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
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

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER, mappedBy = "execution")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("imports")
    private List<DataFeedImport> imports = new ArrayList<>();

    @JsonProperty("workflow_id")
    @Column(name = "WORKFLOW_ID", nullable = true)
    private Long workflowId;

    @JsonIgnore
    @Transient
    private List<Table> runtimeTables = new ArrayList<>();

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

    public void addImport(DataFeedImport feedImport) {
        feedImport.setExecution(this);
        imports.add(feedImport);
    }

    public void addImports(List<DataFeedImport> imports) {
        imports.forEach(this::addImport);
    }

    public void setImports(List<DataFeedImport> imports) {
        this.imports = imports;
    }

    public List<Table> getRunTables() {
        return runtimeTables;
    }

    public void addRuntimeTable(Table table) {
        runtimeTables.add(table);
    }

    public void setRuntimeTables(List<Table> tables) {
        this.runtimeTables = tables;
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

    public static enum Status {
        Inited("inited"), //
        Active("active"), //
        Started("started"), //
        Consolidated("consolidated"), //
        Failed("failed");

        private final String name;
        private static Map<String, Status> nameMap;

        static {
            nameMap = new HashMap<String, Status>();
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
                throw new IllegalArgumentException("Cannot find a data feed excution status with name " + name);
            }
        }
    }
}
