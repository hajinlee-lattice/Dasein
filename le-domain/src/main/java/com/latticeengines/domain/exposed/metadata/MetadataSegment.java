package com.latticeengines.domain.exposed.metadata;

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
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "METADATA_SEGMENT", uniqueConstraints = @UniqueConstraint(columnNames = { "TENANT_ID", "NAME" }))
@Filters({ @Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId") })
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class MetadataSegment implements HasPid, HasName, HasTenantId {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;
    
    @Column(name = "NAME", nullable = false)
    @JsonProperty("name")
    private String name;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;
    
    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;
    
    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TABLE_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Table table;
    
    @Transient
    @JsonProperty("table_name")
    private String tableName;
    
    @JsonProperty(value = "table_name", required = false)
    public String getTableName() {
        return tableName;
    }
    
    @JsonProperty(value = "table_name", required = false)
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        if (table != null) {
            setTableName(table.getName());
        } else {
            setTableName(null);
        }
        
        this.table = table;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public Tenant getTenant() {
        return tenant;
    }
    
    public void setTenant(Tenant tenant) {
        if (tenant != null) {
            this.tenant = tenant;
            setTenantId(tenant.getPid());
        }
    }


}
