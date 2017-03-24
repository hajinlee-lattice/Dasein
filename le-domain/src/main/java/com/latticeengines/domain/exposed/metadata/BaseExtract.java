package com.latticeengines.domain.exposed.metadata;

import javax.persistence.Column;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.MappedSuperclass;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.security.HasTenantId;

@MappedSuperclass
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
public abstract class BaseExtract implements HasName, HasTenantId {

    @Column(name = "NAME", nullable = false)
    @JsonProperty("name")
    private String name;

    @Column(name = "PATH", nullable = false, length = 2048)
    @JsonProperty("path")
    private String path;

    @JsonIgnore
    @ManyToOne
    @JoinColumn(name = "FK_TABLE_ID", nullable = false)
    private Table table;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @Column(name = "PROCESSED_RECORDS", nullable = false)
    @JsonProperty("processed_records")
    private Long processedRecords;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public Long getProcessedRecords() {
        return processedRecords;
    }

    public void setProcessedRecords(long processedRecords) {
        this.processedRecords = processedRecords;
    }
}
