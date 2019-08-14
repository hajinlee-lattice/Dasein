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
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.TableGenerator;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

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
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
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

    @JsonProperty("launchCompletionPercent")
    @Column(name = "LAUNCH_COMPLETION_PERCENT")
    private double launchCompletionPercent;

    @JsonProperty("accountsSelected")
    @Column(name = "ACCOUNTS_SELECTED")
    private Long accountsSelected;

    @JsonProperty("contactsSelected")
    @Column(name = "CONTACTS_SELECTED")
    private Long contactsSelected;

    @JsonProperty("contactsLaunched")
    @Column(name = "CONTACTS_LAUNCHED")
    private Long contactsLaunched;

    @JsonProperty("accountsLaunched")
    @Column(name = "ACCOUNTS_LAUNCHED")
    private Long accountsLaunched;

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

    @JsonProperty("destinationSysType")
    @Column(name = "DESTINATION_SYS_TYPE")
    @Enumerated(EnumType.STRING)
    private CDLExternalSystemType destinationSysType;

    @JsonProperty("destinationAccountId")
    @Column(name = "DESTINATION_ACC_ID")
    private String destinationAccountId;

    @JsonProperty("audienceId")
    @Column(name = "AUDIENCE_ID")
    private String audienceId;

    @JsonProperty("audienceName")
    @Column(name = "AUDIENCE_NAME")
    private String audienceName;

    @JsonProperty("folderName")
    @Column(name = "FOLDER_NAME")
    private String folderName;

    @JsonProperty("exportFile")
    @Transient
    private String exportFile;

    @JsonProperty("deleted")
    @Column(name = "DELETED", nullable = false)
    private Boolean deleted = Boolean.FALSE;

    @JsonProperty("channelConfig")
    @Column(name = "CHANNEL_CONFIG")
    @Lob
    private String channelConfig;

    @JsonProperty("addAccountsTable")
    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_ADD_ACCOUNTS_TABLE")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private com.latticeengines.domain.exposed.metadata.Table addAccountsTable;

    @JsonProperty("addContactsTable")
    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_ADD_CONTACTS_TABLE")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private com.latticeengines.domain.exposed.metadata.Table addContactsTable;

    @JsonProperty("removeAccountsTable")
    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_REMOVE_ACCOUNTS_TABLE")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private com.latticeengines.domain.exposed.metadata.Table removeAccountsTable;

    @JsonProperty("removeContactsTable")
    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_REMOVE_CONTACTS_TABLE")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private com.latticeengines.domain.exposed.metadata.Table removeContactsTable;

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

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
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

    public Long getContactsLaunched() {
        return this.contactsLaunched;
    }

    public void setContactsLaunched(Long contactsLaunched) {
        this.contactsLaunched = contactsLaunched;
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

    public com.latticeengines.domain.exposed.metadata.Table getAddAccountsTable() {
        return addAccountsTable;
    }

    public void setAddAccountsTable(com.latticeengines.domain.exposed.metadata.Table addAccountsTable) {
        this.addAccountsTable = addAccountsTable;
    }

    public com.latticeengines.domain.exposed.metadata.Table getAddContactsTable() {
        return addContactsTable;
    }

    public void setAddContactsTable(com.latticeengines.domain.exposed.metadata.Table addContactsTable) {
        this.addContactsTable = addContactsTable;
    }

    public com.latticeengines.domain.exposed.metadata.Table getRemoveAccountsTable() {
        return removeAccountsTable;
    }

    public void setRemoveAccountsTable(com.latticeengines.domain.exposed.metadata.Table removeAccountsTable) {
        this.removeAccountsTable = removeAccountsTable;
    }

    public com.latticeengines.domain.exposed.metadata.Table getRemoveContactsTable() {
        return removeContactsTable;
    }

    public void setRemoveContactsTable(com.latticeengines.domain.exposed.metadata.Table removeContactsTable) {
        this.removeContactsTable = removeContactsTable;
    }

}
