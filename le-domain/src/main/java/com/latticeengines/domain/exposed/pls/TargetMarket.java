package com.latticeengines.domain.exposed.pls;

import java.util.Date;

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
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.query.Restriction;
import com.latticeengines.common.exposed.query.Sort;
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
    private Sort intentSort;
    private Integer numProspectsDesired;
    private Integer numDaysBetweenIntentProspectResends;
    private IntentScore intentScoreThreshold;
    private Double fitScoreThreshold;
    private String modelId;
    private String eventColumnName;
    private Boolean deliverProspectsFromExistingAccounts;
    private Boolean isDefault;
    private Restriction accountFilter;
    private Restriction contactFilter;
    private Integer offset;
    private Integer maxProspectsPerAccount;

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

    @Column(name = "DESCRIPTION", nullable = false)
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
    @Temporal(TemporalType.DATE)
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

    @Column(name = "ACCOUNT_FILTER", nullable = true)
    @JsonIgnore
    public String getAccountFilterString() {
        return JsonUtils.serialize(this.accountFilter);
    }

    public void setAccountFilterString(String accountFilterString) {
        this.accountFilter = JsonUtils.deserialize(accountFilterString, Restriction.class);
    }

    @JsonProperty
    @Transient
    public Restriction getAccountFilter() {
        return this.accountFilter;
    }

    public void setAccountFilter(Restriction accountFilter) {
        this.accountFilter = accountFilter;
    }

    @Column(name = "CONTACT_FILTER", nullable = true)
    @JsonIgnore
    public String getContactFilterString() {
        return JsonUtils.serialize(this.contactFilter);
    }

    @JsonIgnore
    public void setContactFilterString(String contactFilterString) {
        this.contactFilter = JsonUtils.deserialize(contactFilterString, Restriction.class);
    }

    @JsonProperty
    @Transient
    public Restriction getContactFilter() {
        return this.contactFilter;
    }

    public void setContactFilter(Restriction contactFilter) {
        this.contactFilter = contactFilter;
    }

    @JsonProperty
    @Transient
    public Sort getIntentSort() {
        return this.intentSort;
    }

    @JsonProperty
    public void setIntentSort(Sort sort) {
        this.intentSort = sort;
    }

    @JsonProperty
    @Column(name = "INTENT_SORT", nullable = false)
    public String getIntentSortString() {
        return JsonUtils.serialize(this.intentSort);
    }

    @JsonProperty
    public void setIntentSortString(String intentSortStr) {
        this.intentSort = JsonUtils.deserialize(intentSortStr, Sort.class);
    }

    @Column(name = "NUM_PROSPECTS_DESIRED", nullable = true)
    @JsonProperty
    public Integer getNumProspectsDesired() {
        return this.numProspectsDesired;
    }

    @JsonProperty
    public void setNumProspectsDesired(Integer numProspectsDesired) {
        this.numProspectsDesired = numProspectsDesired;
    }

    @Column(name = "NUM_DAYS_BETWEEN_INTENT_PROSPECT_RESENDS", nullable = true)
    @JsonProperty
    public Integer getNumDaysBetweenIntentProspectResends() {
        return this.numDaysBetweenIntentProspectResends;
    }

    @JsonProperty
    public void setNumDaysBetweenIntentProspectResends(Integer numDaysBetweenIntentProspectResends) {
        this.numDaysBetweenIntentProspectResends = numDaysBetweenIntentProspectResends;
    }

    @JsonProperty
    @Column(name = "INTENT_SCORE_THRESHOLD", nullable = false)
    @Enumerated(EnumType.STRING)
    public IntentScore getIntentScoreThreshold() {
        return intentScoreThreshold;
    }

    @JsonProperty
    public void setIntentScoreThreshold(IntentScore intentScoreThreshold) {
        this.intentScoreThreshold = intentScoreThreshold;
    }

    @Column(name = "FIT_SCORE_THRESHOLD", nullable = true)
    @JsonProperty
    public Double getFitScoreThreshold() {
        return this.fitScoreThreshold;
    }

    @JsonProperty
    public void setFitScoreThreshold(Double fitScoreThreshold) {
        this.fitScoreThreshold = fitScoreThreshold;
    }

    @Column(name = "MODEL_ID", nullable = true)
    @JsonProperty
    public String getModelId() {
        return this.modelId;
    }

    @JsonProperty
    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    @Column(name = "EVENT_COLUMN_NAME", nullable = true)
    @JsonProperty
    public String getEventColumnName() {
        return this.eventColumnName;
    }

    @JsonProperty
    public void setEventColumnName(String eventColumnName) {
        this.eventColumnName = eventColumnName;
    }

    @Column(name = "DELIVER_PROSPECTS_FROM_EXISTING_ACCOUNTS", nullable = false)
    @JsonProperty
    public Boolean isDeliverProspectsFromExistingAccounts() {
        return this.deliverProspectsFromExistingAccounts;
    }

    @JsonProperty
    public void setDeliverProspectsFromExistingAccounts(Boolean deliverProspectsFromExistingAccounts) {
        this.deliverProspectsFromExistingAccounts = deliverProspectsFromExistingAccounts;
    }

    @Column(name = "MAX_PROSPECTS_PER_ACCOUNT", nullable = true)
    @JsonProperty
    public Integer getMaxProspectsPerAccount() {
        return this.maxProspectsPerAccount;
    }

    @JsonProperty
    public void setMaxProspectsPerAccount(Integer maxProspectsPerAccount) {
        this.maxProspectsPerAccount = maxProspectsPerAccount;
    }

    @Column(name = "IS_DEFAULT")
    @JsonProperty
    public Boolean getIsDefault() {
        return this.isDefault;
    }

    @JsonProperty
    public void setIsDefault(Boolean isDefault) {
        this.isDefault = isDefault;
    }

    @Column(name = "OFFSET", nullable = false)
    @JsonProperty
    public Integer getOffset() {
        return this.offset;
    }

    @JsonProperty
    public void setOffset(Integer offset) {
        this.offset = offset;
    }

}
