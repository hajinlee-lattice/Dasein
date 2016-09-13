package com.latticeengines.domain.exposed.pls;

import java.util.List;

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
import javax.persistence.Table;
import javax.persistence.Transient;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Index;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "ENRICHMENT")
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class Enrichment implements HasPid, HasId<String>, HasTenant, HasTenantId {

    private Long pid;
    private String id;
    private Long tenantId;
    private Tenant tenant;
    private List<MarketoMatchField> marketoMatchFields;
    private String tenantCredentialGUID;

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
    @JsonProperty("Id")
    @Column(name = "ENRICHMENT_ID", unique = true, nullable = false)
    @Index(name = "ENRICHMENT_ID_IDX")
    public String getId() {
        return id;
    }

    @Override
    @JsonProperty("Id")
    public void setId(String id) {
        this.id = id;
    }

    @Override
    @JsonProperty
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    @JsonProperty
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    @Override
    @JsonProperty
    @Column(name = "TENANT_ID", nullable = false)
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    @JsonProperty
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @OneToMany(cascade = CascadeType.MERGE, mappedBy = "enrichment", fetch = FetchType.EAGER)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("MarketoMatchFields")
    public List<MarketoMatchField> getMarketoMatchFields() {
        return marketoMatchFields;
    }

    public void setMarketoMatchFields(List<MarketoMatchField> marketoMatchFields) {
        this.marketoMatchFields = marketoMatchFields;
    }

    @Transient
    @JsonProperty("tenantCredentialGUID")
    public String getTenantCredentialGUID() {
        return tenantCredentialGUID;
    }

    @JsonProperty("tenantCredentialGUID")
    public void setTenantCredentialGUID(String tenantCredentialGUID) {
        this.tenantCredentialGUID = tenantCredentialGUID;
    }

}
