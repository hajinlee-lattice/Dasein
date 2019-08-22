package com.latticeengines.domain.exposed.pls;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;
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
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.FilterDefs;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.ParamDef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.TalkingPoint;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.ratings.coverage.RatingBucketCoverage;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "PLAY", indexes = { //
        @Index(name = "PLAY_DELETED", columnList = "DELETED"), //
        @Index(name = "PLAY_CLEANUP_DONE", columnList = "CLEANUP_DONE") //
})
@JsonIgnoreProperties(ignoreUnknown = true)
@FilterDefs({
        @FilterDef(name = "tenantFilter", defaultCondition = "TENANT_ID = :tenantFilterId", parameters = {
                @ParamDef(name = "tenantFilterId", type = "java.lang.Long") }),
        @FilterDef(name = "softDeleteFilter", defaultCondition = "DELETED !=true") })
@Filters({ @Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId"),
        @Filter(name = "softDeleteFilter", condition = "DELETED != true") })
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "pid")
public class Play implements HasName, HasPid, HasTenantId, HasAuditingFields, SoftDeletable {

    private static final String PLAY_NAME_PREFIX = "play";
    private static final String PLAY_NAME_FORMAT = "%s__%s";

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    @Id
    @JsonProperty("pid")
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
    @Column(name = "DESCRIPTION", length = 255, nullable = true)
    private String description;

    @JsonProperty("ratings")
    @Transient
    private List<RatingBucketCoverage> ratings;

    @JsonProperty("targetSegment")
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_SEGMENT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private MetadataSegment targetSegment;

    @JsonProperty("ratingEngine")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_RATING_ENGINE_ID")
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
    private PlayStatus playStatus = PlayStatus.ACTIVE;

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

    @JsonProperty("updatedBy")
    @Column(name = "UPDATED_BY", nullable = false)
    private String updatedBy;

    @JsonProperty("playType")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_PLAY_TYPE", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private PlayType playType;

    @JsonProperty("playGroups")
    @ManyToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinTable(name = "PLAY_PLAY_GROUP", joinColumns = { @JoinColumn(name = "FK_PLAY_ID") }, inverseJoinColumns = {
            @JoinColumn(name = "FK_PLAY_GROUP_ID") })
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Set<PlayGroup> playGroups;

    @JsonProperty("lastTalkingPointPublishTime")
    @Column(name = "LAST_TALKING_POINT_PUBLISH_TIME")
    @Temporal(TemporalType.TIMESTAMP)
    private Date lastTalkingPointPublishTime;

    @JsonProperty("deleted")
    @Column(name = "DELETED", nullable = false)
    private Boolean deleted = Boolean.FALSE;

    @JsonProperty("isCleanupDone")
    @Column(name = "CLEANUP_DONE", nullable = false)
    private Boolean isCleanupDone = Boolean.FALSE;

    public Play() {
    }

    @Override
    @JsonIgnore
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
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

    public MetadataSegment getTargetSegment() {
        return targetSegment;
    }

    public void setTargetSegment(MetadataSegment targetSegment) {
        this.targetSegment = targetSegment;
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

    @Override
    public Boolean getDeleted() {
        return this.deleted;
    }

    @Override
    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

    public PlayStatus getPlayStatus() {
        return this.playStatus;
    }

    public void setPlayStatus(PlayStatus playStatus) {
        this.playStatus = playStatus;
    }

    @Override
    public Date getCreated() {
        return this.created;
    }

    @Override
    public void setCreated(Date created) {
        this.created = created;
    }

    @Override
    public Date getUpdated() {
        return this.updated;
    }

    @Override
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

    public Boolean getIsCleanupDone() {
        return isCleanupDone;
    }

    public void setIsCleanupDone(Boolean isCleanupDone) {
        this.isCleanupDone = isCleanupDone;
    }

    public PlayType getPlayType() {
        return playType;
    }

    public void setPlayType(PlayType playType) {
        this.playType = playType;
    }

    public Set<PlayGroup> getPlayGroups() {
        return playGroups;
    }

    public void setPlayGroups(Set<PlayGroup> playGroups) {
        this.playGroups = playGroups;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public String generateNameStr() {
        return String.format(PLAY_NAME_FORMAT, PLAY_NAME_PREFIX, UUID.randomUUID().toString());
    }
}
