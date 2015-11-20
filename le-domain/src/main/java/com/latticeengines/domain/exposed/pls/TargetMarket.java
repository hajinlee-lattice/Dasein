package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
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
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

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
    private Long creationTimestamp;
    private Tenant tenant;
    private Long tenantId;
    private Sort intentSort;
    private Integer numProspectsDesired;
    private String modelId;
    private String eventColumnName;
    private Boolean isDefault;
    private Restriction accountFilter;
    private Restriction contactFilter;
    private Integer offset;
    private List<TargetMarketDataFlowOption> rawDataFlowConfiguration = new ArrayList<>();

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
    @Column(name = "CREATION_TIMESTAMP", nullable = false)
    public Long getCreationTimestamp() {
        return this.creationTimestamp;
    }

    @JsonProperty
    public void setCreationTimestamp(Long creationDate) {
        this.creationTimestamp = creationDate;
    }

    @JsonIgnore
    @Transient
    public DateTime getCreationTimestampObject() {
        return new DateTime(creationTimestamp, DateTimeZone.UTC);
    }

    @JsonIgnore
    @Transient
    public void setCreationTimestampObject(DateTime creationTimestamp) {
        DateTime utc = creationTimestamp.toDateTime(DateTimeZone.UTC);
        this.creationTimestamp = utc.getMillis();
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

    @OneToMany(cascade = CascadeType.MERGE, mappedBy = "targetMarket", fetch = FetchType.EAGER)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public List<TargetMarketDataFlowOption> getRawDataFlowConfiguration() {
        return rawDataFlowConfiguration;
    }

    public void setRawDataFlowConfiguration(List<TargetMarketDataFlowOption> rawDataFlowConfiguration) {
        this.rawDataFlowConfiguration = rawDataFlowConfiguration;
    }

    @Transient
    @JsonIgnore
    public TargetMarketDataFlowConfiguration getDataFlowConfiguration() {
        return new TargetMarketDataFlowConfiguration(rawDataFlowConfiguration);
    }

    @Transient
    @JsonIgnore
    public void setDataFlowConfiguration(TargetMarketDataFlowConfiguration configuration) {
        this.rawDataFlowConfiguration = configuration.getBag();
    }
}
