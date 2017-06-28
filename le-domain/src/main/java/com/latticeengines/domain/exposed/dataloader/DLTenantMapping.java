package com.latticeengines.domain.exposed.dataloader;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "DL_TENANT_MAPPING", uniqueConstraints = @UniqueConstraint(columnNames = { "DL_TENANT_ID",
        "DL_LOAD_GROUP" }))
public class DLTenantMapping implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("dl_tenant_id")
    @Column(name = "DL_TENANT_ID")
    private String dlTenantId;

    @JsonProperty("dl_load_group")
    @Column(name = "DL_LOAD_GROUP")
    private String dlLoadGroup;

    @JsonProperty("tenant_id")
    @Column(name = "TENANT_ID", nullable = false)
    private String tenantId;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getDlTenantId() {
        return dlTenantId;
    }

    public void setDlTenantId(String dlTenantId) {
        this.dlTenantId = dlTenantId;
    }

    public String getDlLoadGroup() {
        return dlLoadGroup;
    }

    public void setDlLoadGroup(String dlLoadGroup) {
        this.dlLoadGroup = dlLoadGroup;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }
}
