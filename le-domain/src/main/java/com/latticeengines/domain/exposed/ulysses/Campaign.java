package com.latticeengines.domain.exposed.ulysses;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.persistence.CascadeType;
import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.apache.avro.reflect.AvroIgnore;
import org.apache.avro.reflect.AvroSchema;
import org.apache.avro.reflect.Nullable;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datafabric.DynamoAttribute;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "ULYSSES_CAMPAIGN", //
uniqueConstraints = { @UniqueConstraint(columnNames = { "TENANT_ID", "NAME" }) })
@Filters({ @Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId") })
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class Campaign implements HasPid, HasName, HasTenantId, HasId<String> {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Column(name = "PID", unique = true, nullable = false)
    @AvroIgnore
    private Long pid;

    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonProperty("campaign_type")
    @Column(name = "CAMPAIGN_TYPE", nullable = false)
    @Nullable
    private CampaignType campaignType;

    @Column(name = "TENANT_ID", nullable = false)
    @AvroIgnore
    @JsonIgnore
    private Long tenantId;

    @Column(name = "LAUNCHED", nullable = false)
    private boolean launched = false;

    @JsonProperty("segments")
    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "ULYSSES_CAMPAIGN_SEGMENTS", joinColumns = @JoinColumn(name = "PID"))
    @Column(name = "SEGMENT_NAME")
    private List<String> segments = new ArrayList<>();

    @JsonProperty("insights")
    @DynamoAttribute("insights")
    @Transient
    private List<Insight> insights = new ArrayList<>();

    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @AvroIgnore
    private Tenant tenant;

    @JsonProperty("campaign_id")
    @Column(name = "CAMPAIGN_ID", nullable = false)
    @AvroIgnore
    private String campaignId;

    public Campaign() {
        if (campaignId == null) {
            campaignId = UUID.randomUUID().toString();
        }
    }

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
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public CampaignType getCampaignType() {
        return campaignType;
    }

    public void setCampaignType(CampaignType campaignType) {
        this.campaignType = campaignType;
    }

    public List<String> getSegments() {
        return segments;
    }

    public void setSegments(List<String> segments) {
        this.segments = segments;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    public boolean isLaunched() {
        return launched;
    }

    public void setLaunched(boolean launched) {
        this.launched = launched;
    }

    public void setTenant(Tenant tenant) {
        if (tenant != null) {
            setTenantId(tenant.getPid());
            this.tenant = tenant;
        }
    }

    public Tenant getTenant() {
        return tenant;
    }

    public List<Insight> getInsights() {
        return insights;
    }

    public void setInsights(List<Insight> insights) {
        this.insights = insights;
    }

    @Override
    public String getId() {
        if (campaignType == CampaignType.PROFILE) {
            return tenant.getId() + "|PROFILE";
        }
        return campaignId;
    }

    @Override
    public void setId(String id) {
        this.campaignId = id;
    }

}
