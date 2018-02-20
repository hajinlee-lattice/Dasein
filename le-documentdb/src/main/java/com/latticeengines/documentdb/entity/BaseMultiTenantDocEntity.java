package com.latticeengines.documentdb.entity;

import javax.persistence.Column;
import javax.persistence.MappedSuperclass;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.documentdb.annotation.TenantIdColumn;

@MappedSuperclass
public abstract class BaseMultiTenantDocEntity extends BaseDocumentEntity {

    static final String COLUMN_TENANT_ID = "TenantId";

    @TenantIdColumn
    @JsonProperty("TenantId")
    @Column(name = COLUMN_TENANT_ID, nullable = false)
    private String tenantId;

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

}
