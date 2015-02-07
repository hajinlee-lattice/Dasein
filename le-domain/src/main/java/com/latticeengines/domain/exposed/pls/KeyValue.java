package com.latticeengines.domain.exposed.pls;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;

import org.hibernate.annotations.Filter;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenantId;

@Entity
@Table(name = "KEY_VALUE")
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class KeyValue implements HasTenantId, HasPid {

    private Long pid;
    private Long tenantId;
    private Long entityPid;
    private byte[] data;

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

    @Override
    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    public Long getTenantId() {
        return tenantId;
    }
    
    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Column(name = "DATA", nullable = false)
    @Lob
    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @Column(name = "ENTITY_PID", nullable = false)
    @JsonIgnore
    public Long getEntityPid() {
        return entityPid;
    }

    public void setEntityPid(Long entityPid) {
        this.entityPid = entityPid;
    }

}
