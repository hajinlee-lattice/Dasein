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
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.Table;

@Entity
@javax.persistence.Table(name = "DATAFEED_EXECUTION", uniqueConstraints = @UniqueConstraint(columnNames = {
        "FEED_ID", "EXECUTION" }))
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
    private DataFeed feed;

    @JsonIgnore
    @Column(name = "FEED_ID", nullable = false)
    private Long feedId;

    @Column(name = "EXECUTION", nullable = false)
    @JsonProperty("execution")
    private Long execution;

    @Column(name = "STATUS", nullable = false)
    @JsonProperty("status")
    Status status;

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER, mappedBy = "dataFeedImport")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("imports")
    List<DataFeedImport> imports;

    @JsonProperty("runtimeTabless")
    @Transient
    List<Table> runtimeTables;

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
        return feed;
    }
  
    public void setFeed(DataFeed feed) {
        this.feedId = feed.getPid();
        this.feed = feed;
    }

    public Long getExecution() {
        return execution;
    }
  
    public void setExecution(Long execution) {
        this.execution = execution;
    }

    public List<DataFeedImport> getImports() {
        return imports;
    }

    public void addImport(DataFeedImport feedImport) {
        feedImport.setExecution(this);
        imports.add(feedImport);
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

    public static enum Status {
        Inited("inited"), //
        Active("active"), //
        Consolidated("consolidated");

        private final String name;
        private static Map<String, Status> nameMap;

        static {
            nameMap = new HashMap<String, Status>();
            for (Status status: Status.values()) {
                nameMap.put(status.getName(), status);
            }
        }

        Status(String name) {
            this.name = name;
        }

        public String getName() { return this.name; }

        public String toString() { return this.name; }

        public static Status fromName(String name) {
            if (name == null) {
                return null;
            }
            if (nameMap.containsKey(name)) {
                return nameMap.get(name);
            } else  {
                throw new IllegalArgumentException("Cannot find a data feed excution status with name " + name);
            }
        }
   }
}
