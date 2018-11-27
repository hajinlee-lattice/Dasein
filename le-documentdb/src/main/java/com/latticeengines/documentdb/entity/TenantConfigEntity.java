package com.latticeengines.documentdb.entity;

import java.util.List;

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
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;
import org.hibernate.annotations.TypeDefs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import com.vladmihalcea.hibernate.type.json.JsonStringType;

@Entity
@Table(name = "TenantConfiguration", uniqueConstraints = {
        @UniqueConstraint(name = "UX_NAME", columnNames = { "TenantId", "ContractId" }) })
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@TypeDefs({ @TypeDef(name = "json", typeClass = JsonStringType.class),
        @TypeDef(name = "jsonb", typeClass = JsonBinaryType.class) })
public class TenantConfigEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @JsonIgnore
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("ServiceConfigs")
    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER, mappedBy = "tenantConfigEntity")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private List<ServiceConfigEntity> serviceConfigs;

    @JsonProperty("ContractId")
    @Column(name = "ContractId")
    private String contractId;

    @JsonProperty("TenantId")
    @Column(name = "TenantId")
    private String tenantId;

    @Type(type = "json")
    @JsonProperty("ContractPropertities")
    @Column(name = "ContractPropertities", columnDefinition = "'JSON'")
    private ContractProperties contractPropertities;

    @JsonProperty("DefaultSpace")
    @Column(name = "DefaultSpace")
    private String defaultSpace;

    @Type(type = "json")
    @JsonProperty("TenantProperties")
    @Column(name = "TenantProperties", columnDefinition = "'JSON'")
    private TenantProperties tenantProperties;

    @Type(type = "json")
    @JsonProperty("FeatureFlags")
    @Column(name = "FeatureFlags", columnDefinition = "'JSON'")
    private FeatureFlagValueMap featureFlags;

    @Type(type = "json")
    @JsonProperty("SpaceProperties")
    @Column(name = "SpaceProperties", columnDefinition = "'JSON'")
    private CustomerSpaceProperties spaceProperties;

    @Type(type = "json")
    @JsonProperty("SpaceConfiguration")
    @Column(name = "SpaceConfiguration", columnDefinition = "'JSON'")
    private SpaceConfiguration spaceConfiguration;

    @Enumerated(EnumType.STRING)
    @JsonProperty("Status")
    @Column(name = "Status", nullable = false)
    private TenantStatus status;

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getContractId() {
        return contractId;
    }

    public void setContractId(String contractId) {
        this.contractId = contractId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public ContractProperties getContractPropertities() {
        return contractPropertities;
    }

    public void setContractPropertities(ContractProperties contractPropertities) {
        this.contractPropertities = contractPropertities;
    }

    public String getDefaultSpace() {
        return defaultSpace;
    }

    public void setDefaultSpace(String defaultSpace) {
        this.defaultSpace = defaultSpace;
    }

    public TenantProperties getTenantProperties() {
        return tenantProperties;
    }

    public void setTenantProperties(TenantProperties tenantProperties) {
        this.tenantProperties = tenantProperties;
    }

    public FeatureFlagValueMap getFeatureFlags() {
        return featureFlags;
    }

    public void setFeatureFlags(FeatureFlagValueMap featureFlags) {
        this.featureFlags = featureFlags;
    }

    public CustomerSpaceProperties getSpaceProperties() {
        return spaceProperties;
    }

    public void setSpaceProperties(CustomerSpaceProperties spaceProperties) {
        this.spaceProperties = spaceProperties;
    }

    public SpaceConfiguration getSpaceConfiguration() {
        return spaceConfiguration;
    }

    public void setSpaceConfiguration(SpaceConfiguration spaceConfiguration) {
        this.spaceConfiguration = spaceConfiguration;
    }

    public TenantStatus getStatus() {
        return status;
    }

    public void setStatus(TenantStatus status) {
        this.status = status;
    }

    public List<ServiceConfigEntity> getServiceConfigs() {
        return serviceConfigs;
    }

}
