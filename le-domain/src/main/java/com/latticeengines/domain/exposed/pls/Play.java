package com.latticeengines.domain.exposed.pls;

import java.util.Date;
import java.util.List;
import java.util.UUID;

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
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "PLAY")
@JsonIgnoreProperties(ignoreUnknown = true)
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
@JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "pid")
public class Play implements HasName, HasPid, HasTenantId {

    public static final String PLAY_NAME_PREFIX = "play";
    public static final String PLAY_NAME_FORMAT = "%s__%s";

    public Play() {
        setName(generateNameStr());
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonProperty("displayName")
    @Column(name = "DISPLAY_NAME", nullable = false)
    private String displayName;

    @JsonProperty("description")
    @Column(name = "DESCRIPTION", length = 8192, nullable = true)
    private String description;

    @JsonProperty("segment")
    @Column(name = "SEGMENT_NAME", nullable = true)
    private String segmentName;

    @JsonIgnore
    @Transient
    private MetadataSegment segment;
    //
    // @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    // @OnDelete(action = OnDeleteAction.CASCADE)
    // @JoinColumn(name = "FK_CALL_PREP_ID")
    // private CallPrep callPrep;

    @JsonProperty("rating")
    @Transient
    private RatingObject rating;

    @JsonProperty("launchHistory")
    @Transient
    private LaunchHistory launchHistory;

    @JsonProperty("talkingPoints")
    @OneToMany(cascade = { CascadeType.REMOVE }, orphanRemoval = true, mappedBy = "play", fetch = FetchType.EAGER)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private List<TalkingPoint> talkingPoints;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @JsonProperty("timeStamp")
    @Column(name = "TIMESTAMP", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date timestamp;

    @JsonProperty("lastUpdatedTimestamp")
    @Column(name = "LAST_UPDATED_TIMESTAMP", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date lastUpdatedTimestamp;

    @JsonProperty("createdBy")
    @Column(name = "CREATED_BY", nullable = false)
    private String createdBy;

    @JsonProperty("excludeAccountsWithoutSalesforceId")
    @Column(name = "EXCLUDE_ACCOUNTS_WITHOUT_SFID", nullable = true)
    private Boolean excludeAccountsWithoutSalesforceId = Boolean.FALSE;

    @JsonProperty("excludeContactsWithoutSalesforceId")
    @Column(name = "EXCLUDE_CONTACTS_WITHOUT_SFID", nullable = true)
    private Boolean excludeContactsWithoutSalesforceId = Boolean.FALSE;

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

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getSegmentName() {
        return segmentName;
    }

    public void setSegmentName(String segmentName) {
        this.segmentName = segmentName;
    }

    public MetadataSegment getSegment() {
        return segment;
    }

    public void setSegment(MetadataSegment segment) {
        this.segment = segment;
        if (segment != null) {
            setSegmentName(segment.getName());
        }
    }

    public RatingObject getRating() {
        return rating;
    }

    public void setRating(RatingObject rating) {
        this.rating = rating;
    }

    public List<TalkingPoint> getTalkingPoints() {
        return talkingPoints;
    }

    public void setTalkingPoints(List<TalkingPoint> talkingPoints) {
        this.talkingPoints = talkingPoints;
    }

    @JsonIgnore
    public Tenant getTenant() {
        return this.tenant;
    }

    @JsonIgnore
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
    }

    @Override
    @JsonIgnore
    public Long getTenantId() {
        return this.tenantId;
    }

    @Override
    @JsonIgnore
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public Date getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Date getLastUpdatedTimestamp() {
        return this.lastUpdatedTimestamp;
    }

    public void setLastUpdatedTimestamp(Date lastUpdatedTimestamp) {
        this.lastUpdatedTimestamp = lastUpdatedTimestamp;
    }

    public LaunchHistory getLaunchHistory() {
        return this.launchHistory;
    }

    public void setLaunchHistory(LaunchHistory launchHistory) {
        this.launchHistory = launchHistory;
    }

    public Boolean getExcludeAccountsWithoutSalesforceId() {
        return this.excludeAccountsWithoutSalesforceId;
    }

    public void setExcludeAccountsWithoutSalesforceId(boolean value) {
        this.excludeAccountsWithoutSalesforceId = value;
    }

    public Boolean getExcludeContactsWithoutSalesforceId() {
        return this.excludeContactsWithoutSalesforceId;
    }

    public void setExcludeContactsWithoutSalesforceId(boolean value) {
        this.excludeContactsWithoutSalesforceId = value;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public String generateNameStr() {
        return String.format(PLAY_NAME_FORMAT, PLAY_NAME_PREFIX, UUID.randomUUID().toString());
    }
}
