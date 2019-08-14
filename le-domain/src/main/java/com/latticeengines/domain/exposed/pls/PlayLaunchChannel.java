package com.latticeengines.domain.exposed.pls;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
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
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.NamedAttributeNode;
import javax.persistence.NamedEntityGraph;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.util.CronExpression;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.ParamDef;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.LaunchType;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "PLAY_LAUNCH_CHANNEL")
@JsonIgnoreProperties(ignoreUnknown = true)
@FilterDef(name = "tenantFilter", defaultCondition = "TENANT_ID = :tenantFilterId", parameters = {
        @ParamDef(name = "tenantFilterId", type = "java.lang.Long") })
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
@NamedEntityGraph(name = "PlayLaunchChannel.play", attributeNodes = { @NamedAttributeNode("play") })
public class PlayLaunchChannel implements HasPid, HasId<String>, HasTenantId, HasAuditingFields, SoftDeletable {

    private static final String PLAY_LAUNCH_CHANNEL_NAME_PREFIX = "channel";
    private static final String PLAY_LAUNCH_CHANNEL_NAME_FORMAT = "%s__%s";

    @Id
    @JsonProperty("pid")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("id")
    @Column(name = "ID", nullable = false)
    private String id;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

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

    @JsonProperty("bucketsToLaunch")
    @Column(name = "BUCKETS_TO_LAUNCH")
    @Type(type = "text")
    private String bucketsToLaunch;

    @JsonProperty("launchUnscored")
    @Column(name = "LAUNCH_UNSCORED", nullable = false)
    private boolean launchUnscored = false;

    @JsonProperty("launchType")
    @Column(name = "LAUNCH_TYPE", nullable = false)
    @Enumerated(EnumType.STRING)
    private LaunchType launchType;

    @JsonProperty("maxAccountsToLaunch")
    @Column(name = "MAX_ACCOUNTS_TO_LAUNCH")
    private Long maxAccountsToLaunch;

    @JsonProperty("cronScheduleExpression")
    @Column(name = "CRON_SCHEDULE_EXPRESSION")
    private String cronScheduleExpression;

    @JsonProperty("nextScheduledLaunch")
    @Column(name = "NEXT_SCHEDULED_LAUNCH")
    private Date nextScheduledLaunch;

    @JsonProperty("channelConfig")
    @Column(name = "CHANNEL_CONFIG")
    @Lob
    private String channelConfig;

    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_PLAY_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Play play;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_LOOKUP_ID_MAP_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private LookupIdMap lookupIdMap;

    @JsonProperty("lastLaunch")
    @Transient
    private PlayLaunch lastLaunch;

    @JsonProperty("isAlwaysOn")
    @Column(name = "ALWAYS_ON")
    private Boolean isAlwaysOn = Boolean.FALSE;

    @JsonProperty("deleted")
    @Column(name = "DELETED", nullable = false)
    private Boolean deleted = Boolean.FALSE;

    @JsonProperty("currentLaunchedAccountUniverseTable")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_CURRENT_LAUNCHED_ACCOUNT_UNIVERSE_TABLE")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Table currentLaunchedAccountUniverseTable;

    @JsonProperty("currentLaunchedContactUniverseTable")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_CURRENT_LAUNCHED_CONTACT_UNIVERSE_TABLE")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Table currentLaunchedContactUniverseTable;

    public PlayLaunchChannel() {
    }

    public String generateChannelId() {
        return String.format(PLAY_LAUNCH_CHANNEL_NAME_FORMAT, PLAY_LAUNCH_CHANNEL_NAME_PREFIX,
                UUID.randomUUID().toString());
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
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @JsonIgnore
    public Tenant getTenant() {
        return tenant;
    }

    @JsonIgnore
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
    }

    @JsonIgnore
    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @JsonIgnore
    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public Date getCreated() {
        return created;
    }

    @Override
    public void setCreated(Date created) {
        this.created = created;
    }

    @Override
    public Date getUpdated() {
        return updated;
    }

    @Override
    public void setUpdated(Date updated) {
        this.updated = updated;
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

    @Override
    public Boolean getDeleted() {
        return deleted;
    }

    @Override
    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    public Play getPlay() {
        return play;
    }

    public void setPlay(Play play) {
        this.play = play;
    }

    public LookupIdMap getLookupIdMap() {
        return lookupIdMap;
    }

    public void setLookupIdMap(LookupIdMap lookupIdMap) {
        this.lookupIdMap = lookupIdMap;
    }

    public Boolean getIsAlwaysOn() {
        return isAlwaysOn;
    }

    public void setIsAlwaysOn(Boolean isAlwaysOn) {
        this.isAlwaysOn = isAlwaysOn;
    }

    public Long getMaxAccountsToLaunch() {
        return maxAccountsToLaunch;
    }

    public void setMaxAccountsToLaunch(Long maxAccountsToLaunch) {
        this.maxAccountsToLaunch = maxAccountsToLaunch;
    }

    public Set<RatingBucketName> getBucketsToLaunch() {
        if (StringUtils.isNotBlank(this.bucketsToLaunch)) {
            List<?> attrListIntermediate = JsonUtils.deserialize(this.bucketsToLaunch, List.class);
            return new TreeSet<>(JsonUtils.convertList(attrListIntermediate, RatingBucketName.class));
        }

        return new TreeSet<>();
    }

    public void setBucketsToLaunch(Set<RatingBucketName> bucketsToLaunch) {
        this.bucketsToLaunch = JsonUtils.serialize(bucketsToLaunch);
    }

    public Boolean isLaunchUnscored() {
        return launchUnscored;
    }

    public void setLaunchUnscored(Boolean launchUnscored) {
        this.launchUnscored = launchUnscored;
    }

    public LaunchType getLaunchType() {
        return launchType;
    }

    public void setLaunchType(LaunchType launchType) {
        this.launchType = launchType;
    }

    public String getCronScheduleExpression() {
        return cronScheduleExpression;
    }

    public void setCronScheduleExpression(String cronScheduleExpression) {
        this.cronScheduleExpression = cronScheduleExpression;
    }

    public Date getNextScheduledLaunch() {
        return nextScheduledLaunch;
    }

    public void setNextScheduledLaunch(Date nextScheduledLaunch) {
        this.nextScheduledLaunch = nextScheduledLaunch;
    }

    public PlayLaunch getLastLaunch() {
        return lastLaunch;
    }

    public void setLastLaunch(PlayLaunch lastLaunch) {
        this.lastLaunch = lastLaunch;
    }

    public ChannelConfig getChannelConfig() {
        ChannelConfig newChannelConfig = null;
        if (channelConfig != null) {
            newChannelConfig = JsonUtils.deserialize(channelConfig, ChannelConfig.class);
        }
        return newChannelConfig;
    }

    public void setChannelConfig(ChannelConfig channelConfig) {
        this.channelConfig = JsonUtils.serialize(channelConfig);
    }

    public static Date getNextDateFromCronExpression(PlayLaunchChannel channel) {
        try {
            CronExpression cronExp = new CronExpression(channel.getCronScheduleExpression());
            Date toReturn = cronExp.getNextValidTimeAfter(
                    channel.getNextScheduledLaunch() == null ? new Date() : channel.getNextScheduledLaunch());

            if (toReturn.before(new Date())) {
                toReturn = cronExp.getNextValidTimeAfter(new Date());
            }
            return toReturn;
        } catch (ParseException e) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { String.format("Invalid Cron Schedule %s in Channel for channel id: %s",
                            channel.getCronScheduleExpression(), channel.getId()) });
        }
    }

    public Table getCurrentLaunchedAccountUniverseTable() {
        return currentLaunchedAccountUniverseTable;
    }

    public void setCurrentLaunchedAccountUniverseTable(Table currentLaunchedAccountUniverseTable) {
        this.currentLaunchedAccountUniverseTable = currentLaunchedAccountUniverseTable;
    }

    public Table getCurrentLaunchedContactUniverseTable() {
        return currentLaunchedContactUniverseTable;
    }

    public void setCurrentLaunchedContactUniverseTable(Table currentLaunchedContactUniverseTable) {
        this.currentLaunchedContactUniverseTable = currentLaunchedContactUniverseTable;
    }
}
