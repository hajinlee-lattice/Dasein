package com.latticeengines.domain.exposed.pls;

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
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "MARKETO_MATCH_FIELD", uniqueConstraints = {
        @UniqueConstraint(columnNames = { "TENANT_ID", "MARKETO_MATCH_FIELD_NAME" }) })
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class MarketoMatchField implements HasPid, HasTenant, HasTenantId {

    private Long pid;
    private Long tenantId;
    private Tenant tenant;
    private MarketoMatchFieldName marketoMatchFieldName;
    private String marketoFieldName;
    private Enrichment enrichment;

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
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    @JsonIgnore
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
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

    @JsonProperty("marketo_match_field_name")
    @Enumerated(EnumType.STRING)
    @Column(name = "MARKETO_MATCH_FIELD_NAME", nullable = false)
    public MarketoMatchFieldName getMarketoMatchFieldName() {
        return marketoMatchFieldName;
    }

    public void setMarketoMatchFieldName(MarketoMatchFieldName marketoMatchFieldName) {
        this.marketoMatchFieldName = marketoMatchFieldName;
    }

    @JsonProperty("marketo_field_name")
    @Column(name = "MARKETO_FIELD_NAME", nullable = true)
    public String getMarketoFieldName() {
        return marketoFieldName;
    }

    public void setMarketoFieldName(String marketoFieldName) {
        this.marketoFieldName = marketoFieldName;
    }

    @ManyToOne
    @JoinColumn(name = "ENRICHMENT_ID", nullable = false)
    @JsonIgnore
    public Enrichment getEnrichment() {
        return this.enrichment;
    }

    @JsonIgnore
    public void setEnrichment(Enrichment enrichment) {
        this.enrichment = enrichment;
    }
}
