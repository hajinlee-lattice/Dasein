package com.latticeengines.domain.exposed.pls;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

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
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.multitenant.TalkingPoint;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "PLAY")
@JsonIgnoreProperties(ignoreUnknown = true)
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
@JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "pid")
public class Play implements HasName, HasPid, HasTenantId, HasAuditingFields {

    public static final String PLAY_NAME_PREFIX = "play";
    public static final String PLAY_NAME_FORMAT = "%s__%s";
    public static final String DEFAULT_NAME_PATTERN = "PLAY -- %s";
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    public Play() {
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

    @JsonProperty("ratings")
    @Transient
    private List<RatingBucketCoverage> ratings;

    @JsonProperty("ratingEngine")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_RATING_ENGINE_ID", nullable = true)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private RatingEngine ratingEngine;

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

    @JsonProperty("status")
    @Column(name = "STATUS")
    @Enumerated(EnumType.STRING)
    private PlayStatus playStatus;

    @JsonProperty("created")
    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date created;

    @JsonProperty("updated")
    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated;

    @JsonProperty("createdBy")
    @Column(name = "CREATED_BY", nullable = false)
    private String createdBy;

    @JsonProperty("lastTalkingPointPublishTime")
    @Column(name = "LAST_TALKING_POINT_PUBLISH_TIME")
    @Temporal(TemporalType.TIMESTAMP)
    private Date lastTalkingPointPublishTime;

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

    public RatingEngine getRatingEngine() {
        return ratingEngine;
    }

    public void setRatingEngine(RatingEngine ratingEngine) {
        this.ratingEngine = ratingEngine;
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

    public void setPlayStatus(PlayStatus playStatus) {
        this.playStatus = playStatus;
    }

    public PlayStatus getPlayStatus() {
        return this.playStatus;
    }

    public Date getCreated() {
        return this.created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public Date getUpdated() {
        return this.updated;
    }

    public void setUpdated(Date updated) {
        this.updated = updated;
    }

    public LaunchHistory getLaunchHistory() {
        return this.launchHistory;
    }

    public void setLaunchHistory(LaunchHistory launchHistory) {
        this.launchHistory = launchHistory;
    }

    public Date getLastTalkingPointPublishTime() {
        return lastTalkingPointPublishTime;
    }

    public void setLastTalkingPointPublishTime(Date lastTalkingPointPublishTime) {
        this.lastTalkingPointPublishTime = lastTalkingPointPublishTime;
    }

    public List<RatingBucketCoverage> getRatings() {
        return this.ratings;
    }

    public void setRatings(List<RatingBucketCoverage> ratings) {
        this.ratings = ratings;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public String generateNameStr() {
        return String.format(PLAY_NAME_FORMAT, PLAY_NAME_PREFIX, UUID.randomUUID().toString());
    }
}
