package com.latticeengines.documentdb.entity;

import javax.persistence.Column;
import javax.persistence.MappedSuperclass;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.documentdb.annotation.TenantIdColumn;

@MappedSuperclass
public abstract class BaseMultiTenantDocEntity<T> extends BaseDocumentEntity<T> {

    @TenantIdColumn
    @JsonProperty("TenantId")
    @Column(name = "TenantId", nullable = false)
    private String tenantId;

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    protected String getTenantIdField() {
        return "tenantId";
    }

}
