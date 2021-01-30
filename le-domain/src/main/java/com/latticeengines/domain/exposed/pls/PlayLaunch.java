package com.latticeengines.domain.exposed.pls;

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
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.TableGenerator;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.FilterDefs;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.ParamDef;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.LaunchType;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "PLAY_LAUNCH", indexes = { //
        @Index(name = "PLAY_LAUNCH_ID", columnList = "LAUNCH_ID"), //
        @Index(name = "PLAY_LAUNCH_CREATED", columnList = "CREATED"), //
        @Index(name = "PLAY_LAUNCH_LAST_UPDATED", columnList = "UPDATED"), //
        @Index(name = "PLAY_LAUNCH_STATE", columnList = "STATE"), //
        @Index(name = "PLAY_LAUNCH_DELETED", columnList = "DELETED") //
})
@JsonIgnoreProperties(ignoreUnknown = true)
@FilterDefs({
        @FilterDef(name = "tenantFilter", defaultCondition = "TENANT_ID = :tenantFilterId", parameters = {
                @ParamDef(name = "tenantFilterId", type = "java.lang.Long") }),
        @FilterDef(name = "softDeleteFilter", defaultCondition = "DELETED !=true") })
@Filters({ @Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId"),
        @Filter(name = "softDeleteFilter", condition = "DELETED != true") })
public class PlayLaunch implements HasPid, HasId<String>, HasTenantId, HasAuditingFields, SoftDeletable {

    public static final int PID_INIT_VALUE = 1_000_000; // 1M
    private static final String PLAY_LAUNCH_NAME_PREFIX = "launch";
    private static final String PLAY_LAUNCH_NAME_FORMAT = "%s__%s";
    @Id
    @Basic(optional = false)
    @TableGenerator(name = "PlayLaunch_SEQ_GEN", table = "PLS_MULTITENANT_SEQ_ID", pkColumnName = "SEQUENCE_NAME", valueColumnName = "SEQUENCE_VAL", pkColumnValue = "PlayLaunch_SEQUENCE", initialValue = PID_INIT_VALUE, allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.TABLE, generator = "PlayLaunch_SEQ_GEN")
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("launchId")
    @Column(name = "LAUNCH_ID", unique = true, nullable = false)
    private String launchId;

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

    @JsonProperty("launchState")
    @Column(name = "STATE", nullable = false)
    @Enumerated(EnumType.STRING)
    private LaunchState launchState;

    @JsonIgnore
    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_PLAY_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Play play;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonIgnore
    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_PLAY_LAUNCH_CHANNEL_ID")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private PlayLaunchChannel playLaunchChannel;

    @JsonProperty("applicationId")
    @Column(name = "APPLICATION_ID")
    private String applicationId;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @JsonProperty("parentDeltaCalculationWorkflowId")
    @Column(name = "FK_DELTA_CALC_WORKFLOW_ID")
    private Long parentDeltaWorkflowId;

    @JsonProperty("launchWorkflowId")
    @Column(name = "FK_WORKFLOW_ID")
    private Long launchWorkflowId;

    @JsonProperty("isScheduledLaunch")
    @Column(name = "IS_SCHEDULED_LAUNCH", nullable = false, columnDefinition = "'BIT DEFAULT 0'")
    private boolean isScheduledLaunch = false;

    @JsonProperty("addAccountsTable")
    @Column(name = "ADD_ACCOUNTS_TABLE_NAME")
    private String addAccountsTable;

    @JsonProperty("addContactsTable")
    @Column(name = "ADD_CONTACTS_TABLE_NAME")
    private String addContactsTable;

    @JsonProperty("removeAccountsTable")
    @Column(name = "REMOVE_ACCOUNTS_TABLE_NAME")
    private String removeAccountsTable;

    @JsonProperty("removeContactsTable")
    @Column(name = "REMOVE_CONTACTS_TABLE_NAME")
    private String removeContactsTable;

    @JsonProperty("completeContactsTable")
    @Column(name = "COMPLETE_CONTACTS_TABLE_NAME")
    private String completeContactsTable;

    // # Launch Config
    @JsonProperty("launchType")
    @Column(name = "LAUNCH_TYPE", nullable = false)
    @Enumerated(EnumType.STRING)
    private LaunchType launchType;

    @JsonProperty("excludeItemsWithoutSalesforceId")
    @Column(name = "EXCLUDE_ITEMS_WITHOUT_SFID", nullable = false)
    private Boolean excludeItemsWithoutSalesforceId = Boolean.FALSE;

    @JsonProperty("topNCount")
    @Column(name = "TOP_N_COUNT")
    private Long topNCount;

    @JsonProperty("bucketsToLaunch")
    @Column(name = "BUCKETS_TO_LAUNCH")
    @Type(type = "text")
    private String bucketsToLaunch;

    @JsonProperty("launchUnscored")
    @Column(name = "LAUNCH_UNSCORED", nullable = false)
    private boolean launchUnscored = false;

    @JsonProperty("table_name")
    @Column(name = "TABLE_NAME")
    private String tableName;

    @JsonProperty("destinationOrgId")
    @Column(name = "DESTINATION_ORG_ID")
    private String destinationOrgId;

    @Transient
    @JsonProperty("destinationOrgName")
    private String destinationOrgName;

    @Column(name = "DESTINATION_SYS_NAME")
    @JsonProperty("destinationSysName")
    @Enumerated(EnumType.STRING)
    private CDLExternalSystemName destinationSysName;

    @JsonProperty("destinationSysType")
    @Column(name = "DESTINATION_SYS_TYPE")
    @Enumerated(EnumType.STRING)
    private CDLExternalSystemType destinationSysType;

    @JsonProperty("destinationAccountId")
    @Column(name = "DESTINATION_ACC_ID")
    private String destinationAccountId;

    @JsonProperty("destinationContactId")
    @Column(name = "DESTINATION_CONTACT_ID")
    private String destinationContactId;

    @JsonProperty("audienceId")
    @Column(name = "AUDIENCE_ID")
    private String audienceId;

    @JsonProperty("audienceName")
    @Column(name = "AUDIENCE_NAME")
    private String audienceName;

    @JsonProperty("folderId")
    @Column(name = "FOLDER_ID")
    private String folderId;

    @JsonProperty("folderName")
    @Column(name = "FOLDER_NAME")
    private String folderName;

    @JsonProperty("exportFile")
    @Transient
    private String exportFile;

    @JsonProperty("deleted")
    @Column(name = "DELETED", nullable = false)
    private Boolean deleted = Boolean.FALSE;

    @JsonProperty("audienceSize")
    @Column(name = "AUDIENCE_SIZE")
    private Long audienceSize;

    @JsonProperty("matchedCount")
    @Column(name = "MATCHED_COUNT")
    private Long matchedCount;

    @JsonProperty("channelConfig")
    @Column(name = "CHANNEL_CONFIG")
    @Lob
    private String channelConfig;

    @JsonProperty("audienceState")
    @Transient
    private String audienceState;
    // # Launch Config

    // # LaunchStats
    @JsonProperty("launchCompletionPercent")
    @Column(name = "LAUNCH_COMPLETION_PERCENT")
    private double launchCompletionPercent;

    // segment selected accounts
    @JsonProperty("accountsSelected")
    @Column(name = "ACCOUNTS_SELECTED")
    private Long accountsSelected;

    // segment selected contacts
    @JsonProperty("contactsSelected")
    @Column(name = "CONTACTS_SELECTED")
    private Long contactsSelected;

    // accumulative launched contacts
    @JsonProperty("contactsLaunched")
    @Column(name = "CONTACTS_LAUNCHED")
    private Long contactsLaunched;

    // incremental added contacts
    @JsonProperty("contactsAdded")
    @Column(name = "CONTACTS_ADDED")
    private Long contactsAdded;

    // incremental deleted contacts
    @JsonProperty("contactsDeleted")
    @Column(name = "CONTACTS_DELETED")
    private Long contactsDeleted;

    // accumulative launched accounts
    @JsonProperty("accountsLaunched")
    @Column(name = "ACCOUNTS_LAUNCHED")
    private Long accountsLaunched;

    // incremental added accounts
    @JsonProperty("accountsAdded")
    @Column(name = "ACCOUNTS_ADDED")
    private Long accountsAdded;

    // incremental deleted accounts
    @JsonProperty("accountsDeleted")
    @Column(name = "ACCOUNTS_DELETED")
    private Long accountsDeleted;

    @JsonProperty("accountsSuppressed")
    @Column(name = "ACCOUNTS_SUPPRESSED")
    private Long accountsSuppressed;

    @JsonProperty("contactsSuppressed")
    @Column(name = "CONTACTS_SUPPRESSED")
    private Long contactsSuppressed;

    @JsonProperty("accountsErrored")
    @Column(name = "ACCOUNTS_ERRORED")
    private Long accountsErrored;

    @JsonProperty("contactsErrored")
    @Column(name = "CONTACTS_ERRORED")
    private Long contactsErrored;

    @JsonProperty("accountsDuplicated")
    @Column(name = "ACCOUNTS_DUPLICATED")
    private Long accountsDuplicated;

    @JsonProperty("contactsDuplicated")
    @Column(name = "CONTACTS_DUPLICATED")
    private Long contactsDuplicated;

    @JsonProperty("matchedRate")
    @Transient
    private Long matchedRate;

    @JsonProperty("addRecommendationsTable")
    @Column(name = "ADD_RECOMMENDATIONS_TABLE_NAME")
    private String addRecommendationsTable;

    @JsonProperty("deleteRecommendationsTable")
    @Column(name = "DELETE_RECOMMENDATIONS_TABLE_NAME")
    private String deleteRecommendationsTable;

    @JsonProperty("tapType")
    @Enumerated(EnumType.STRING)
    @Column(name = "TAP_TYPE")
    private Play.TapType tapType;

    @JsonProperty("recordsStats")
    @Column(name = "RECORDS_STATS")
    @Lob
    private String recordsStats;

    public PlayLaunch() {
    }

    public static String generateLaunchId() {
        return String.format(PLAY_LAUNCH_NAME_FORMAT, PLAY_LAUNCH_NAME_PREFIX, UUID.randomUUID().toString());
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getLaunchId() {
        return launchId;
    }

    public void setLaunchId(String id) {
        this.launchId = id;
    }

    @Override
    public String getId() {
        return launchId;
    }

    @Override
    public void setId(String launchId) {
        this.launchId = launchId;
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

    public LaunchState getLaunchState() {
        return launchState;
    }

    public void setLaunchState(LaunchState launchState) {
        this.launchState = launchState;
    }

    @JsonProperty("uiLaunchState")
    public String getUILaunchState() {
        return launchState != null ? launchState.toUILaunchState(destinationSysName) : null;
    }

    public Play getPlay() {
        return play;
    }

    public void setPlay(Play play) {
        this.play = play;
    }

    public PlayLaunchChannel getPlayLaunchChannel() {
        return playLaunchChannel;
    }

    public void setPlayLaunchChannel(PlayLaunchChannel playLaunchChannel) {
        this.playLaunchChannel = playLaunchChannel;
    }

    public Long getParentDeltaWorkflowId() {
        return parentDeltaWorkflowId;
    }

    public void setParentDeltaWorkflowId(Long parentDeltaWorkflowId) {
        this.parentDeltaWorkflowId = parentDeltaWorkflowId;
    }

    public Long getLaunchWorkflowId() {
        return launchWorkflowId;
    }

    public void setLaunchWorkflowId(Long launchWorkflowId) {
        this.launchWorkflowId = launchWorkflowId;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public boolean isScheduledLaunch() {
        return isScheduledLaunch;
    }

    public void setScheduledLaunch(boolean scheduledLaunch) {
        isScheduledLaunch = scheduledLaunch;
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

    public double getLaunchCompletionPercent() {
        return launchCompletionPercent;
    }

    public void setLaunchCompletionPercent(double launchCompletionPercent) {
        this.launchCompletionPercent = launchCompletionPercent;
    }

    public Long getAccountsSelected() {
        return accountsSelected;
    }

    public void setAccountsSelected(Long accountsSelected) {
        this.accountsSelected = accountsSelected;
    }

    public Long getAccountsLaunched() {
        return this.accountsLaunched;
    }

    public void setAccountsLaunched(Long accountsLaunched) {
        this.accountsLaunched = accountsLaunched;
    }

    public Long getAccountsAdded() {
        return this.accountsAdded;
    }

    public void setAccountsAdded(Long accountsAdded) {
        this.accountsAdded = accountsAdded;
    }

    public Long getAccountsDeleted() {
        return this.accountsDeleted;
    }

    public void setAccountsDeleted(Long accountsDeleted) {
        this.accountsDeleted = accountsDeleted;
    }

    public Long getContactsLaunched() {
        return this.contactsLaunched;
    }

    public void setContactsLaunched(Long contactsLaunched) {
        this.contactsLaunched = contactsLaunched;
    }

    public Long getContactsAdded() {
        return this.contactsAdded;
    }

    public void setContactsAdded(Long contactsAdded) {
        this.contactsAdded = contactsAdded;
    }

    public Long getContactsDeleted() {
        return this.contactsDeleted;
    }

    public void setContactsDeleted(Long contactsDeleted) {
        this.contactsDeleted = contactsDeleted;
    }

    public Long getAccountsSuppressed() {
        return accountsSuppressed;
    }

    public void setAccountsSuppressed(Long accountsSuppressed) {
        this.accountsSuppressed = accountsSuppressed;
    }

    public Long getAccountsErrored() {
        return accountsErrored;
    }

    public void setAccountsErrored(Long accountsErrored) {
        this.accountsErrored = accountsErrored;
    }

    public Long getContactsSelected() {
        return contactsSelected;
    }

    public void setContactsSelected(Long contactsSelected) {
        this.contactsSelected = contactsSelected;
    }

    public Long getContactsSuppressed() {
        return contactsSuppressed;
    }

    public void setContactsSuppressed(Long contactsSuppressed) {
        this.contactsSuppressed = contactsSuppressed;
    }

    public Long getContactsErrored() {
        return contactsErrored;
    }

    public void setContactsErrored(Long contactsErrored) {
        this.contactsErrored = contactsErrored;
    }

    public Long getAccountsDuplicated() {
        return accountsDuplicated;
    }

    public void setAccountsDuplicated(Long accountsDuplicated) {
        this.accountsDuplicated = accountsDuplicated;
    }

    public Long getContactsDuplicated() {
        return contactsDuplicated;
    }

    public void setContactsDuplicated(Long contactsDuplicated) {
        this.contactsDuplicated = contactsDuplicated;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public LaunchType getLaunchType() {
        return launchType;
    }

    public void setLaunchType(LaunchType launchType) {
        this.launchType = launchType;
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

    public boolean isLaunchUnscored() {
        return launchUnscored;
    }

    public void setLaunchUnscored(boolean launchUnscored) {
        this.launchUnscored = launchUnscored;
    }

    public Boolean getExcludeItemsWithoutSalesforceId() {
        return this.excludeItemsWithoutSalesforceId;
    }

    public void setExcludeItemsWithoutSalesforceId(Boolean excludeItemsWithoutSalesforceId) {
        this.excludeItemsWithoutSalesforceId = excludeItemsWithoutSalesforceId;
    }

    public Long getTopNCount() {
        return topNCount;
    }

    public void setTopNCount(Long topNCount) {
        this.topNCount = topNCount;
    }

    public String getDestinationOrgId() {
        return destinationOrgId;
    }

    public void setDestinationOrgId(String destinationOrgId) {
        this.destinationOrgId = destinationOrgId;
    }

    public String getDestinationOrgName() {
        return destinationOrgName;
    }

    public void setDestinationOrgName(String destinationOrgName) {
        this.destinationOrgName = destinationOrgName;
    }

    public CDLExternalSystemType getDestinationSysType() {
        return destinationSysType;
    }

    public void setDestinationSysType(CDLExternalSystemType destinationSysType) {
        this.destinationSysType = destinationSysType;
    }

    public String getDestinationAccountId() {
        return destinationAccountId;
    }

    public void setDestinationAccountId(String destinationAccountId) {
        this.destinationAccountId = destinationAccountId;
    }

    public String getDestinationContactId() {
        return destinationContactId;
    }

    public void setDestinationContactId(String destinationContactId) {
        this.destinationContactId = destinationContactId;
    }

    @Override
    public Boolean getDeleted() {
        return deleted;
    }

    @Override
    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public String getAudienceId() {
        return audienceId;
    }

    public void setAudienceId(String audienceId) {
        this.audienceId = audienceId;
    }

    public String getAudienceName() {
        return audienceName;
    }

    public void setAudienceName(String audienceName) {
        this.audienceName = audienceName;
    }

    public String getFolderId() {
        return folderId;
    }

    public void setFolderId(String folderId) {
        this.folderId = folderId;
    }

    public String getFolderName() {
        return folderName;
    }

    public void setFolderName(String folderName) {
        this.folderName = folderName;
    }

    public String getExportFile() {
        return exportFile;
    }

    public void setExportFile(String exportFile) {
        this.exportFile = exportFile;
    }

    public Long getAudienceSize() {
        return audienceSize;
    }

    public void setAudienceSize(Long audienceSize) {
        this.audienceSize = audienceSize;
    }

    public Long getMatchedCount() {
        return matchedCount;
    }

    public void setMatchedCount(Long matchedCount) {
        this.matchedCount = matchedCount;
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

    public String getAddAccountsTable() {
        return addAccountsTable;
    }

    public void setAddAccountsTable(String addAccountsTable) {
        this.addAccountsTable = addAccountsTable;
    }

    public String getAddContactsTable() {
        return addContactsTable;
    }

    public void setAddContactsTable(String addContactsTable) {
        this.addContactsTable = addContactsTable;
    }

    public String getRemoveAccountsTable() {
        return removeAccountsTable;
    }

    public void setRemoveAccountsTable(String removeAccountsTable) {
        this.removeAccountsTable = removeAccountsTable;
    }

    public String getRemoveContactsTable() {
        return removeContactsTable;
    }

    public void setRemoveContactsTable(String removeContactsTable) {
        this.removeContactsTable = removeContactsTable;
    }

    public String getCompleteContactsTable() {
        return completeContactsTable;
    }

    public void setCompleteContactsTable(String completeContactsTable) {
        this.completeContactsTable = completeContactsTable;
    }

    public void setAudienceState(String audienceState) {
        this.audienceState = audienceState;
    }

    public String getAudienceState() {
        return audienceState;
    }

    public void setMatchedRate(Long matchedRate) {
        this.matchedRate = matchedRate;
    }

    public Long getMatchedRate() {
        return matchedRate;
    }

    public CDLExternalSystemName getDestinationSysName() {
        return destinationSysName;
    }

    public void setDestinationSysName(CDLExternalSystemName destinationSysName) {
        this.destinationSysName = destinationSysName;
    }

    public RecordsStats getRecordsStats() {
        RecordsStats newRecordStats = null;
        if (recordsStats != null) {
            newRecordStats = JsonUtils.deserialize(recordsStats, RecordsStats.class);
        }
        return newRecordStats;
    }

    public void setRecordsStats(RecordsStats recordsStats) {
        this.recordsStats = JsonUtils.serialize(recordsStats);
    }

    public void merge(PlayLaunch playLaunch) {
        if (playLaunch.getLaunchState() != null) {
            this.setLaunchState(playLaunch.getLaunchState());
        }
        if (StringUtils.isNotBlank(playLaunch.getApplicationId())) {
            this.setApplicationId(playLaunch.getApplicationId());
        }
        if (StringUtils.isNotBlank(playLaunch.getTableName())) {
            this.setTableName(playLaunch.getTableName());
        }
        if (playLaunch.getParentDeltaWorkflowId() != null) {
            this.setParentDeltaWorkflowId(playLaunch.getParentDeltaWorkflowId());
        }
        if (playLaunch.getLaunchWorkflowId() != null) {
            this.setLaunchWorkflowId(playLaunch.getLaunchWorkflowId());
        }

        // Account stats
        if (playLaunch.getAccountsSelected() != null) {
            this.setAccountsSelected(playLaunch.getAccountsSelected());
        }
        if (playLaunch.getAccountsLaunched() != null) {
            this.setAccountsLaunched(playLaunch.getAccountsLaunched());
        }
        if (playLaunch.getAccountsAdded() != null) {
            this.setAccountsAdded(playLaunch.getAccountsAdded());
        }
        if (playLaunch.getAccountsDeleted() != null) {
            this.setAccountsDeleted(playLaunch.getAccountsDeleted());
        }
        if (playLaunch.getAccountsSuppressed() != null) {
            this.setAccountsSuppressed(playLaunch.getAccountsSuppressed());
        }
        if (playLaunch.getAccountsErrored() != null) {
            this.setAccountsErrored(playLaunch.getAccountsErrored());
        }
        if (playLaunch.getAccountsDuplicated() != null) {
            this.setAccountsDuplicated(playLaunch.getAccountsDuplicated());
        }
        if (playLaunch.getLaunchType() != null) {
            this.setLaunchType(playLaunch.getLaunchType());
        }

        this.setLaunchCompletionPercent(playLaunch.getLaunchCompletionPercent());

        // Contact stats
        if (playLaunch.getContactsSelected() != null) {
            this.setContactsSelected(playLaunch.getContactsSelected());
        }
        if (playLaunch.getContactsLaunched() != null) {
            this.setContactsLaunched(playLaunch.getContactsLaunched());
        }
        if (playLaunch.getContactsAdded() != null) {
            this.setContactsAdded(playLaunch.getContactsAdded());
        }
        if (playLaunch.getContactsDeleted() != null) {
            this.setContactsDeleted(playLaunch.getContactsDeleted());
        }
        if (playLaunch.getContactsSuppressed() != null) {
            this.setContactsSuppressed(playLaunch.getContactsSuppressed());
        }
        if (playLaunch.getContactsErrored() != null) {
            this.setContactsErrored(playLaunch.getContactsErrored());
        }
        if (playLaunch.getContactsDuplicated() != null) {
            this.setContactsDuplicated(playLaunch.getContactsDuplicated());
        }

        // Tray System properties
        if (StringUtils.isNotBlank(playLaunch.getAudienceId())) {
            this.setAudienceId(playLaunch.getAudienceId());
        }
        if (StringUtils.isNotBlank(playLaunch.getAudienceName())) {
            this.setAudienceName(playLaunch.getAudienceName());
        }
        if (playLaunch.getAudienceSize() != null) {
            this.setAudienceSize(playLaunch.getAudienceSize());
        }
        if (StringUtils.isNotBlank(playLaunch.getFolderId())) {
            this.setFolderId(playLaunch.getFolderId());
        }
        if (StringUtils.isNotBlank(playLaunch.getFolderName())) {
            this.setFolderName(playLaunch.getFolderName());
        }
        if (playLaunch.getMatchedCount() != null) {
            this.setMatchedCount(playLaunch.getMatchedCount());
        }

        // Delta Launch Tables
        if (StringUtils.isNotBlank(playLaunch.getAddAccountsTable())) {
            this.setAddAccountsTable(playLaunch.getAddAccountsTable());
        }
        if (StringUtils.isNotBlank(playLaunch.getCompleteContactsTable())) {
            this.setCompleteContactsTable(playLaunch.getCompleteContactsTable());
        }
        if (StringUtils.isNotBlank(playLaunch.getRemoveAccountsTable())) {
            this.setRemoveAccountsTable(playLaunch.getRemoveAccountsTable());
        }
        if (StringUtils.isNotBlank(playLaunch.getAddContactsTable())) {
            this.setAddContactsTable(playLaunch.getAddContactsTable());
        }
        if (StringUtils.isNotBlank(playLaunch.getRemoveContactsTable())) {
            this.setRemoveContactsTable(playLaunch.getRemoveContactsTable());
        }
        if (StringUtils.isNotBlank(playLaunch.getAddRecommendationsTable())) {
            this.setAddRecommendationsTable(playLaunch.getAddRecommendationsTable());
        }
        if (StringUtils.isNotBlank(playLaunch.getDeleteRecommendationsTable())) {
            this.setDeleteRecommendationsTable(playLaunch.getDeleteRecommendationsTable());
        }

        // Launch Configuration
        if (CollectionUtils.isNotEmpty(playLaunch.getBucketsToLaunch())) {
            this.setBucketsToLaunch(playLaunch.getBucketsToLaunch());
        }
        if (StringUtils.isNotBlank(playLaunch.getDestinationAccountId())) {
            this.setDestinationAccountId(playLaunch.getDestinationAccountId());
        }
        if (StringUtils.isNotBlank(playLaunch.getDestinationOrgId())) {
            this.setDestinationOrgId(playLaunch.getDestinationOrgId());
        }
        if (playLaunch.getDestinationSysType() != null) {
            this.setDestinationSysType(playLaunch.getDestinationSysType());
        }
        if (playLaunch.getDestinationOrgName() != null) {
            this.setDestinationOrgName(playLaunch.getDestinationOrgName());
        }
        if (playLaunch.getDestinationSysName() != null) {
            this.setDestinationSysName(playLaunch.getDestinationSysName());
        }
        if (playLaunch.getExcludeItemsWithoutSalesforceId() != null) {
            this.setExcludeItemsWithoutSalesforceId(playLaunch.getExcludeItemsWithoutSalesforceId());
        }

        if (StringUtils.isNotBlank(playLaunch.getUpdatedBy())) {
            this.setUpdatedBy(playLaunch.getUpdatedBy());
        }
        if (playLaunch.getTapType() != null) {
            this.setTapType(playLaunch.getTapType());
        }
    }

    public String getAddRecommendationsTable() {
        return addRecommendationsTable;
    }

    public void setAddRecommendationsTable(String addRecommendationsTable) {
        this.addRecommendationsTable = addRecommendationsTable;
    }

    public String getDeleteRecommendationsTable() {
        return deleteRecommendationsTable;
    }

    public void setDeleteRecommendationsTable(String deleteRecommendationsTable) {
        this.deleteRecommendationsTable = deleteRecommendationsTable;
    }

    public Play.TapType getTapType() {
        return tapType;
    }

    public void setTapType(Play.TapType tapType) {
        this.tapType = tapType;
    }
}
