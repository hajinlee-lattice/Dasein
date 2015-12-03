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
import javax.persistence.OneToOne;
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
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasApplicationId;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "TARGET_MARKET")
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class TargetMarket implements HasPid, HasName, HasTenant, HasTenantId, HasApplicationId {

    private Long pid;
    private String name;
    private String description;
    private Long creationTimestamp;
    private Tenant tenant;
    private Long tenantId;
    private List<String> selectedIntent;
    private Integer numProspectsDesired;
    private String modelId;
    private String eventColumnName;
    private Boolean isDefault;
    private Restriction accountFilter;
    private Restriction contactFilter;
    private Integer offset;
    private List<TargetMarketDataFlowOption> rawDataFlowConfiguration = new ArrayList<>();
    private TargetMarketStatistics targetMarketStatistics;
    private String applicationId;

    @Column(name = "NAME", nullable = false)
    @Override
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @Column(name = "DESCRIPTION", nullable = false)
    @JsonProperty("description")
    public String getDescription() {
        return this.description;
    }

    @JsonProperty("description")
    public void setDescription(String description) {
        this.description = description;
    }

    @JsonProperty("creation_timestamp")
    @Column(name = "CREATION_TIMESTAMP", nullable = false)
    public Long getCreationTimestamp() {
        return this.creationTimestamp;
    }

    @JsonProperty("creation_timestamp")
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

    @JsonIgnore
    public void setAccountFilterString(String accountFilterString) {
        this.accountFilter = JsonUtils.deserialize(accountFilterString, Restriction.class);
    }

    @JsonProperty("account_filter")
    @Transient
    public Restriction getAccountFilter() {
        return this.accountFilter;
    }

    @JsonProperty("account_filter")
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

    @JsonProperty("contact_filter")
    @Transient
    public Restriction getContactFilter() {
        return this.contactFilter;
    }

    @JsonProperty("contact_filter")
    public void setContactFilter(Restriction contactFilter) {
        this.contactFilter = contactFilter;
    }

    @JsonProperty("selected_intent")
    @Transient
    public List<String> getSelectedIntent() {
        return this.selectedIntent;
    }

    @JsonProperty("selected_intent")
    @Transient
    public void setSelectedIntent(List<String> selectedIntent) {
        this.selectedIntent = selectedIntent;
    }

    @JsonIgnore
    @Column(name = "SELECTED_INTENT", nullable = false)
    public String getSelectedIntentString() {
        return JsonUtils.serialize(this.selectedIntent);
    }

    @JsonIgnore
    @SuppressWarnings("unchecked")
    public void setSelectedIntentString(String selectedIntentString) {
        this.selectedIntent = JsonUtils.deserialize(selectedIntentString, List.class);
    }

    @Column(name = "NUM_PROSPECTS_DESIRED", nullable = true)
    @JsonProperty("num_prospects_desired")
    public Integer getNumProspectsDesired() {
        return this.numProspectsDesired;
    }

    @JsonProperty("num_prospects_desired")
    public void setNumProspectsDesired(Integer numProspectsDesired) {
        this.numProspectsDesired = numProspectsDesired;
    }

    @Column(name = "MODEL_ID", nullable = true)
    @JsonProperty("model_id")
    public String getModelId() {
        return this.modelId;
    }

    @JsonProperty("model_id")
    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    @Column(name = "EVENT_COLUMN_NAME", nullable = true)
    @JsonProperty("event_column_name")
    public String getEventColumnName() {
        return this.eventColumnName;
    }

    @JsonProperty("event_column_name")
    public void setEventColumnName(String eventColumnName) {
        this.eventColumnName = eventColumnName;
    }

    @Column(name = "IS_DEFAULT")
    @JsonProperty("is_default")
    public Boolean getIsDefault() {
        return this.isDefault;
    }

    @JsonProperty("is_default")
    public void setIsDefault(Boolean isDefault) {
        this.isDefault = isDefault;
    }

    @Column(name = "OFFSET", nullable = false)
    @JsonProperty("offset")
    public Integer getOffset() {
        return this.offset;
    }

    @JsonProperty("offset")
    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    @JsonProperty("target_market_statistics")
    @JoinColumn(name = "FK_PID", nullable = false)
    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public TargetMarketStatistics getTargetMarketStatistics() {
        return this.targetMarketStatistics;
    }

    @JsonProperty("target_market_statistics")
    public void setTargetMarketStatistics(TargetMarketStatistics targetMarketStatistics) {
        this.targetMarketStatistics = targetMarketStatistics;
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

    @Column(name = "APPLICATION_ID", nullable = true)
    @JsonProperty("application_id")
    public String getApplicationId() {
        return this.applicationId;
    }

    @JsonProperty("application_id")
    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }
}
