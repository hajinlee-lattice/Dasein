package com.latticeengines.domain.exposed.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
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
import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "METADATA_EXTRACT")
public class Extract implements HasName, HasPid, HasTenantId, GraphNode, Serializable {

    private Long pid;
    private String name;
    private String path;
    private Long extractionTimestamp;
    private Table table;
    private Long tenantId;
    private Long processedRecords;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Column(name = "NAME", unique = false, nullable = false)
    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Column(name = "PATH", unique = false, nullable = false)
    @JsonProperty("path")
    public String getPath() {
        return path;
    }

    @JsonProperty("path")
    public void setPath(String path) {
        this.path = path;
    }

    @Column(name = "EXTRACTION_TS", nullable = false)
    @JsonProperty("extraction_ts")
    public Long getExtractionTimestamp() {
        return extractionTimestamp;
    }

    @JsonProperty("extraction_ts")
    public void setExtractionTimestamp(Long extractionTimestamp) {
        this.extractionTimestamp = extractionTimestamp;
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }

    @Override
    @JsonIgnore
    @Transient
    public Collection<? extends GraphNode> getChildren() {
        return new ArrayList<>();
    }

    @Override
    @JsonIgnore
    @Transient
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        return new HashMap<>();
    }

    @JsonIgnore
    @ManyToOne
    @JoinColumn(name = "FK_TABLE_ID", nullable = false)
    public Table getTable() {
        return table;
    }

    @JsonIgnore
    public void setTable(Table table) {
        this.table = table;
    }

    @Override
    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
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
        if (tenant != null) {
            setTenantId(tenant.getPid());
        }

    }

    @Column(name = "PROCESSED_RECORDS", unique = false, nullable = false)
    @JsonProperty("processed_records")
    public Long getProcessedRecords() {
        return processedRecords;
    }

    @JsonProperty("processed_records")
    public void setProcessedRecords(long processedRecords) {
        this.processedRecords = processedRecords;
    }

}
