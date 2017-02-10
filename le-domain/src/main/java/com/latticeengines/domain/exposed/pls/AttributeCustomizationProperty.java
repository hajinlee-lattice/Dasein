package com.latticeengines.domain.exposed.pls;

import javax.persistence.Access;
import javax.persistence.AccessType;
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
import org.hibernate.annotations.Index;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Access(AccessType.FIELD)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@Table(name = "ATTRIBUTE_CUSTOMIZATION_PROPERTY", //
        uniqueConstraints = { @UniqueConstraint(columnNames = { "TENANT_PID", "ATTRIBUTE_NAME", "USE_CASE", "CATEGORY_NAME",
                "PROPERTY_NAME" }) })
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
public class AttributeCustomizationProperty implements HasPid, HasName, HasTenant, HasTenantId {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonIgnore
    @Index(name = "IX_TENANT_ATTRIBUTENAME_USECASE")
    private Tenant tenant;

    @JsonProperty("attribute_name")
    @Column(name = "ATTRIBUTE_NAME", nullable = false, length = 100)
    @Index(name = "IX_TENANT_ATTRIBUTENAME_USECASE")
    private String attributeName;

    @JsonProperty("use_case")
    @Enumerated(EnumType.STRING)
    @Column(name = "USE_CASE", nullable = false)
    @Index(name = "IX_TENANT_ATTRIBUTENAME_USECASE")
    private AttributeUseCase useCase;

    @JsonIgnore
    @Column(name = "TENANT_PID", nullable = false)
    private Long tenantId;

    @JsonProperty("category_name")
    @Column(name = "CATEGORY_NAME", nullable = false)
    private String categoryName;

    @JsonProperty("property_name")
    @Column(name = "PROPERTY_NAME")
    private String propertyName;

    @JsonProperty("property_value")
    @Column(name = "PROPERTY_VALUE")
    private String propertyValue;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String getName() {
        return attributeName;
    }

    @Override
    public void setName(String name) {
        this.attributeName = name;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
    }

    public Long getTenantId() {
        return tenantId;
    }

    @Override
    @JsonIgnore
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public AttributeUseCase getUseCase() {
        return useCase;
    }

    public void setUseCase(AttributeUseCase useCase) {
        this.useCase = useCase;
    }

    public String getCategoryName() {
        return this.categoryName;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    public String getPropertyName() {
        return this.propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    public String getPropertyValue() {
        return this.propertyValue;
    }

    public void setPropertyValue(String propertyValue) {
        this.propertyValue = propertyValue;
    }
}
