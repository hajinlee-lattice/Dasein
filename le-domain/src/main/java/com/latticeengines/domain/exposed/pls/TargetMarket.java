package com.latticeengines.domain.exposed.pls;

import java.util.Date;

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
import javax.persistence.Table;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.query.Restriction;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "TARGET_MARKET")
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class TargetMarket implements HasPid, HasName, HasTenant, HasTenantId {

    private Long pid;
    private String name;
    private String description;
    private Date creationDate;
    private Tenant tenant;
    private Long tenantId;
    // intent sortspec
    private Integer numProspectsDesired;
    private Integer numDaysBetweenIntentProspectResends;
    private Double intentScoreThreshold;
    private Double fitScoreThreshold;
    private String modelId;
    private String eventColumnName;
    private Boolean shouldGetContactsFromExistingCustomers;
    private Restriction accountFilter;
    private Restriction contactFilter;

    @Column(name = "NAME", nullable = false)
    @Override
    @JsonProperty
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty
    public void setName(String name) {
        this.name = name;
    }
    
    @Column(name="DESCRIPTION", nullable=false)
    @JsonProperty
    public String getDescription() {
        return this.description;
    }

    @JsonProperty
    public void setDescription(String description) {
        this.description = description;
    }
    
    @JsonProperty
    @Column(name = "CREATION_DATE", nullable = false)
    public Date getCreationDate() {
        return this.creationDate;
    }

    @JsonProperty
    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @JsonIgnore
    @Override
    @Column(name = "PID", unique = true, nullable = false)
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
    @JsonIgnore
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;

        if (tenant != null) {
            setTenantId(tenant.getPid());
        }

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
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Column(name="ACCOUNT_FILTER", nullable = true)
    @JsonProperty
    public String getAccountFilter() {
        return JsonUtils.serialize(this.accountFilter);
    }
    
    @JsonProperty
    public void setAccountFilter(String accountFilterString) {
        this.accountFilter = JsonUtils.deserialize(accountFilterString, Restriction.class);
    }
    
    public void setAccountFilter(Restriction accountFilter) {
        this.accountFilter = accountFilter;
    }
    
    @Column(name="CONTACT_FILTER", nullable = true)
    @JsonProperty
    public String getContactFilter() {
        return JsonUtils.serialize(this.contactFilter);
    }
    
    @JsonProperty
    public void setContactFilter(String contactFilterString) {
        this.contactFilter = JsonUtils.deserialize(contactFilterString, Restriction.class);
    }
    
    public void setContactFilter(Restriction contactFilter) {
        this.contactFilter = contactFilter;
    }

    @Column(name="NUM_PROSPECTS_DESIRED", nullable = true)
    @JsonProperty
    public Integer getNumProspectsDesired() {
        return this.numProspectsDesired;
    }
    
    @JsonProperty
    public void setNumProspectsDesired(Integer numProspectsDesired) {
        this.numProspectsDesired = numProspectsDesired;
    }
    
    @Column(name="NUM_DAYS_BETWEEN_INTENT_PROSPECT_RESENDS", nullable = true)
    @JsonProperty
    public Integer getNumDaysBetweenIntentProspectResends() {
        return this.numDaysBetweenIntentProspectResends;
    }
    
    @JsonProperty
    public void setNumDaysBetweenIntentProspectResends(Integer numDaysBetweenIntentProspectResends) {
        this.numDaysBetweenIntentProspectResends = numDaysBetweenIntentProspectResends;
    }

    @JsonProperty
    @Column(name="INTENT_SCORE_THRESHOLD", nullable = true)
    public Double getIntentScoreThreshold() {
        return this.intentScoreThreshold;
    }
    
    @JsonProperty
    public void setIntentScoreThreshold(Double intentScoreThreshold) {
        this.intentScoreThreshold = intentScoreThreshold;
    }

    @Column(name="FIT_SCORE_THRESHOLD", nullable = true)
    @JsonProperty
    public Double getFitScoreThreshold() {
        return this.fitScoreThreshold;
    }
    
    @JsonProperty
    public void setFitScoreThreshold(Double fitScoreThreshold) {
        this.fitScoreThreshold = fitScoreThreshold;
    }
    
    @Column(name="MODEL_ID", nullable = true)
    @JsonProperty
    public String getModelId() {
        return this.modelId;
    }
    
    @JsonProperty
    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    @Column(name="EVENT_COLUMN_NAME", nullable = true)
    @JsonProperty
    public String getEventColumnName() {
        return this.eventColumnName;
    }
    
    @JsonProperty
    public void setEventColumnName(String eventColumnName) {
        this.eventColumnName = eventColumnName;
    }
    
    @Column(name="SHOULD_GET_CONTACTS_FROM_EXISTING_CUSTOMERS", nullable = false)
    @JsonProperty
    public Boolean isShouldGetContactsFromExistingCustomers() {
        return this.shouldGetContactsFromExistingCustomers;
    }
    
    @JsonProperty
    public void setShouldGetContactsFromExistingCustomers(Boolean shouldGetContactsFromExistingCustomers) {
        this.shouldGetContactsFromExistingCustomers = shouldGetContactsFromExistingCustomers;
    }
    
}
