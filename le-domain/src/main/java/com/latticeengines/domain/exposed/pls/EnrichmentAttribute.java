package com.latticeengines.domain.exposed.pls;

import java.io.Serializable;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Access(AccessType.FIELD)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@Table(name = "ENRICHMENT_ATTRIBUTER", uniqueConstraints = { @UniqueConstraint(columnNames = { "TENANT_PID", "COLUMN_ID" }) })
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
public class EnrichmentAttribute implements HasPid, HasTenant, Serializable {

    @Id
    @JsonProperty("PID")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("ColumnId")
    @Column(name = "COLUMN_ID", nullable = false, length = 64)
    private String columnId;

    @JsonIgnore
    @Column(name = "TENANT_PID", nullable = false)
    private Long tenantPid;

    @JsonProperty("TenantId")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    public EnrichmentAttribute(String columnId, Tenant tenant) {
        this.setColumnId(columnId);
        this.setTenant(tenant);
    }

    // for json de-ser
    private EnrichmentAttribute() {
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @JsonIgnore
    private Long getTenantPid() {
        return tenantPid;
    }

    @JsonIgnore
    private void setTenantPid(Long tenantPid) {
        this.tenantPid = tenantPid;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        setTenantPid(tenant.getPid());
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    public String getColumnId() {
        return columnId;
    }

    public void setColumnId(String columnId) {
        this.columnId = columnId;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31).append(columnId).append(tenant.getId()).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof EnrichmentAttribute))
            return false;
        if (obj == this)
            return true;

        EnrichmentAttribute rhs = (EnrichmentAttribute) obj;
        return new EqualsBuilder() //
                .append(columnId, rhs.getColumnId()) //
                .append(tenant.getId(), rhs.getTenant().getId()) //
                .isEquals();
    }

}
