package com.latticeengines.domain.exposed.pls;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.domain.exposed.security.Tenant;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class DeltaCampaignLaunchSparkContext implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String DEFAULT_JOIN_KEY = InterfaceName.AccountId.name();

    @JsonProperty("JoinKey")
    private String joinKey;

    @JsonProperty("Tenant")
    private long tenantPid;

    @JsonProperty("PlayName")
    private String playName;

    @JsonProperty("PlayDisplayName")
    private String playDisplayName;

    @JsonProperty("PlayLaunchId")
    private String playLaunchId;

    @JsonProperty("Created")
    private Date created;

    @JsonProperty("TopNCount")
    private Long topNCount;

    @JsonProperty("PlayDescription")
    private String playDescription;

    @JsonProperty("LaunchTimestampMillis")
    private long launchTimestampMillis;

    @JsonProperty("RatingId")
    private String ratingId;

    @JsonProperty("RatingEngineDisplayName")
    private String ratingEngineDisplayName;

    @JsonProperty("SegmentDisplayName")
    private String segmentDisplayName;

    @JsonProperty("ModelId")
    private String modelId;

    @JsonProperty("ModelSummaryId")
    private String modelSummaryId;

    @JsonProperty("SynchronizationDestination")
    private String synchronizationDestination;

    @JsonProperty("DestinationSysType")
    private String destinationSysType;

    @JsonProperty("DestinationOrgId")
    private String destinationOrgId;

    @JsonProperty("DestinationSysName")
    private String destinationSysName;

    @JsonProperty("DestinationOrgName")
    private String destinationOrgName;

    @JsonProperty("SfdcAccountID")
    private String sfdcAccountID;

    @JsonProperty("SfdcContactID")
    private String sfdcContactID;

    @JsonProperty("AccountColsRecIncluded")
    private List<String> accountColsRecIncluded;

    @JsonProperty("AccountColsRecNotIncludedStd")
    private List<String> accountColsRecNotIncludedStd;

    @JsonProperty("AccountColsRecNotIncludedNonStd")
    private List<String> accountColsRecNotIncludedNonStd;

    @JsonProperty("ContactCols")
    private List<String> contactCols;

    @JsonProperty("DataDbDriver")
    private String dataDbDriver;

    @JsonProperty("DataDbUrl")
    private String dataDbUrl;

    @JsonProperty("DataDbUser")
    private String dataDbUser;

    @JsonProperty("DataDbPassword")
    private String dataDbPassword;

    @JsonProperty("SaltHint")
    private String saltHint;

    @JsonProperty("EncryptionKey")
    private String encryptionKey;

    @JsonProperty("CreateRecommendationDataFrame")
    private boolean createRecommendationDataFrame;

    @JsonProperty("CreateAddCsvDataFrame")
    private boolean createAddCsvDataFrame;

    @JsonProperty("CreateDeleteCsvDataFrame")
    private boolean createDeleteCsvDataFrame;

    @JsonProperty("createTaskDescriptionFile")
    private boolean createTaskDescriptionFile;

    @JsonProperty("publishRecommendationsToDB")
    private boolean publishRecommendationsToDB;

    @JsonProperty("useCustomerId")
    private boolean useCustomerId;

    @JsonProperty("isEntityMatch")
    private boolean isEntityMatch;

    @JsonProperty("shouldDefaultPopulateIds")
    private boolean shouldDefaultPopulateIds;

    public DeltaCampaignLaunchSparkContext() {
    }

    public DeltaCampaignLaunchSparkContext(Tenant tenant, String playName, String playLaunchId, PlayLaunch playLaunch,
            Play play, RatingEngine ratingEngine, MetadataSegment segment, long launchTimestampMillis, String ratingId,
            RatingModel publishedIteration, List<String> accountColsRecIncluded,
            List<String> accountColsRecNotIncludedStd, List<String> accountColsRecNotIncludedNonStd,
            List<String> contactCols, String dataDbDriver, String dataDbUrl, String dataDbUser, String dataDbPassword,
            String saltHint, String encryptionKey) {
        super();
        this.joinKey = DEFAULT_JOIN_KEY;
        this.tenantPid = tenant.getPid();
        this.playName = playName;
        this.playLaunchId = playLaunchId;
        this.created = playLaunch.getCreated();
        this.topNCount = playLaunch.getTopNCount();
        this.playDescription = play.getDescription();
        this.playDisplayName = play.getDisplayName();
        this.launchTimestampMillis = launchTimestampMillis;
        this.ratingId = ratingId;
        this.modelId = publishedIteration != null ? publishedIteration.getId() : null;
        RatingEngineType ratingEngineType = ratingEngine != null ? ratingEngine.getType() : null;
        this.ratingEngineDisplayName = ratingEngine != null ? ratingEngine.getDisplayName() : null;
        this.segmentDisplayName = segment != null ? segment.getDisplayName() : null;
        this.modelSummaryId = publishedIteration != null && RatingEngineType.RULE_BASED != ratingEngineType
                ? ((AIModel) publishedIteration).getModelSummaryId()
                : "";
        this.accountColsRecIncluded = accountColsRecIncluded;
        this.accountColsRecNotIncludedStd = accountColsRecNotIncludedStd;
        this.accountColsRecNotIncludedNonStd = accountColsRecNotIncludedNonStd;
        this.contactCols = contactCols;
        this.dataDbDriver = dataDbDriver;
        this.dataDbUrl = dataDbUrl;
        this.dataDbUser = dataDbUser;
        this.dataDbPassword = dataDbPassword;
        this.saltHint = saltHint;
        this.encryptionKey = encryptionKey;
        setSyncDestination(playLaunch);
    }

    public String getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(String joinKey) {
        this.joinKey = joinKey;
    }

    public long getTenantPid() {
        return tenantPid;
    }

    public String getPlayName() {
        return playName;
    }

    public void setPlayName(String playName) {
        this.playName = playName;
    }

    public String getPlayDisplayName() {
        return playDisplayName;
    }

    public void setPlayDisplayName(String playDisplayName) {
        this.playDisplayName = playDisplayName;
    }

    public String getPlayLaunchId() {
        return playLaunchId;
    }

    public void setPlayLaunchId(String playLaunchId) {
        this.playLaunchId = playLaunchId;
    }

    public long getLaunchTimestampMillis() {
        return launchTimestampMillis;
    }

    public void setLaunchTimestampMillis(long launchTimestampMillis) {
        this.launchTimestampMillis = launchTimestampMillis;
    }

    public Date getCreated() {
        return this.created;
    }

    public Long getTopNCount() {
        return this.topNCount;
    }

    public String getPlayDescription() {
        return this.playDescription;
    }

    public String getRatingId() {
        return this.ratingId;
    }

    public void setRatingId(String ratingId) {
        this.ratingId = ratingId;
    }

    public String getRatingEngineDisplayName() {
        return this.ratingEngineDisplayName;
    }

    public void setRatingEngineDisplayName(String ratingEngineDisplayName) {
        this.ratingEngineDisplayName = ratingEngineDisplayName;
    }

    public String getSegmentDisplayName() {
        return this.segmentDisplayName;
    }

    public void setSegmentDisplayName(String segmentDisplayName) {
        this.segmentDisplayName = segmentDisplayName;
    }

    public String getSynchronizationDestination() {
        return this.synchronizationDestination;
    }

    public void setSynchronizationDestination(String synchronizationDestination) {
        this.synchronizationDestination = synchronizationDestination;
    }

    public String getDestinationSysType() {
        return this.destinationSysType;
    }

    public void setDestinationSysType(String destinationSysType) {
        this.destinationSysType = destinationSysType;
    }

    public String getDestinationOrgId() {
        return this.destinationOrgId;
    }

    public void setDestinationOrgId(String destinationOrgId) {
        this.destinationOrgId = destinationOrgId;
    }

    public String getDestinationOrgName() {
        return this.destinationOrgName;
    }

    public String getSfdcAccountID() {
        return this.sfdcAccountID;
    }

    public void setSfdcAccountID(String sfdcAccountID) {
        this.sfdcAccountID = sfdcAccountID;
    }

    public String getSfdcContactID() {
        return this.sfdcContactID;
    }

    public void setSfdcContactID(String sfdcContactID) {
        this.sfdcContactID = sfdcContactID;
    }

    public List<String> getAccountColsRecIncluded() {
        return this.accountColsRecIncluded;
    }

    public void setAccountColsRecIncluded(List<String> accountColsRecIncluded) {
        this.accountColsRecIncluded = accountColsRecIncluded;
    }

    public List<String> getAccountColsRecNotIncludedStd() {
        return this.accountColsRecNotIncludedStd;
    }

    public void setAccountColsRecNotIncludedStd(List<String> accountColsRecNotIncludedStd) {
        this.accountColsRecNotIncludedStd = accountColsRecNotIncludedStd;
    }

    public List<String> getAccountColsRecNotIncludedNonStd() {
        return this.accountColsRecNotIncludedNonStd;
    }

    public void setAccountColsRecNotIncludedNonStd(List<String> accountColsRecNotIncludedNonStd) {
        this.accountColsRecNotIncludedNonStd = accountColsRecNotIncludedNonStd;
    }

    public List<String> getContactCols() {
        return this.contactCols;
    }

    public void setContactCols(List<String> contactCols) {
        this.contactCols = contactCols;
    }

    public String getModelId() {
        return this.modelId;
    }

    public String getModelSummaryId() {
        return this.modelSummaryId;
    }

    public String getDataDbDriver() {
        return this.dataDbDriver;
    }

    public void setDataDbDriver(String dataDbDriver) {
        this.dataDbDriver = dataDbDriver;
    }

    public String getDataDbUrl() {
        return this.dataDbUrl;
    }

    public void setDataDbUrl(String dataDbUrl) {
        this.dataDbUrl = dataDbUrl;
    }

    public String getDataDbUser() {
        return dataDbUser;
    }

    public void setDataDbUser(String dataDbUser) {
        this.dataDbUser = dataDbUser;
    }

    public String getDataDbPassword() {
        return dataDbPassword;
    }

    public void setDataDbPassword(String dataDbPassword) {
        this.dataDbPassword = dataDbPassword;
    }

    public String getSaltHint() {
        return this.saltHint;
    }

    public void setSaltHint(String saltHint) {
        this.saltHint = saltHint;
    }

    public String getEncryptionKey() {
        return this.encryptionKey;
    }

    public void setEncryptionKey(String encryptionKey) {
        this.encryptionKey = encryptionKey;
    }

    public boolean getCreateRecommendationDataFrame() {
        return this.createRecommendationDataFrame;
    }

    public void setCreateRecommendationDataFrame(boolean createRecommendationDataFrame) {
        this.createRecommendationDataFrame = createRecommendationDataFrame;
    }

    public boolean getCreateAddCsvDataFrame() {
        return this.createAddCsvDataFrame;
    }

    public void setCreateAddCsvDataFrame(boolean createAddCsvDataFrame) {
        this.createAddCsvDataFrame = createAddCsvDataFrame;
    }

    public boolean getCreateDeleteCsvDataFrame() {
        return this.createDeleteCsvDataFrame;
    }

    public void setCreateDeleteCsvDataFrame(boolean createDeleteCsvDataFrame) {
        this.createDeleteCsvDataFrame = createDeleteCsvDataFrame;
    }

    public boolean getCreateTaskDescriptionFile() {
        return this.createTaskDescriptionFile;
    }

    public void setCreateTaskDescriptionFile(boolean createTaskDescriptionFile) {
        this.createTaskDescriptionFile = createTaskDescriptionFile;
    }

    public boolean getPublishRecommendationsToDB() {
        return publishRecommendationsToDB;
    }

    public void setPublishRecommendationsToDB(boolean publishRecommendationsToDB) {
        this.publishRecommendationsToDB = publishRecommendationsToDB;
    }

    private void setSyncDestination(PlayLaunch playLaunch) {
        String synchronizationDestination;
        String destinationSysType;
        if (playLaunch.getDestinationSysType() == null
                || playLaunch.getDestinationSysType() == CDLExternalSystemType.CRM) {
            synchronizationDestination = SynchronizationDestinationEnum.SFDC.name();
            destinationSysType = CDLExternalSystemType.CRM.name();
        } else if (playLaunch.getDestinationSysType() == CDLExternalSystemType.MAP) {
            synchronizationDestination = SynchronizationDestinationEnum.MAP.name();
            destinationSysType = CDLExternalSystemType.MAP.name();
        } else if (playLaunch.getDestinationSysType() == CDLExternalSystemType.FILE_SYSTEM) {
            synchronizationDestination = SynchronizationDestinationEnum.FILE_SYSTEM.name();
            destinationSysType = CDLExternalSystemType.FILE_SYSTEM.name();
        } else if (playLaunch.getDestinationSysType() == CDLExternalSystemType.ADS) {
            synchronizationDestination = SynchronizationDestinationEnum.ADS.name();
            destinationSysType = CDLExternalSystemType.ADS.name();
        } else {
            throw new RuntimeException(String.format("Destination type %s is not supported yet",
                    playLaunch.getDestinationSysType().name()));
        }
        this.synchronizationDestination = synchronizationDestination;
        if (StringUtils.isNotBlank(playLaunch.getDestinationOrgId())) {
            this.destinationOrgId = playLaunch.getDestinationOrgId();
            this.destinationSysName = playLaunch.getDestinationSysName().name();
            this.destinationOrgName = playLaunch.getDestinationOrgName();
            this.destinationSysType = destinationSysType;
        }
        if (StringUtils.isNotBlank(playLaunch.getDestinationAccountId())) {
            String destinationAccountId = playLaunch.getDestinationAccountId().trim();
            this.sfdcAccountID = destinationAccountId;
        } else {
            this.sfdcAccountID = null;
        }

        if (StringUtils.isNotBlank(playLaunch.getDestinationContactId())) {
            String destinationContactId = playLaunch.getDestinationContactId().trim();
            this.sfdcContactID = destinationContactId;
        } else {
            this.sfdcContactID = null;
        }
    }

    public String getDestinationSysName() {
        return destinationSysName;
    }

    public void setDestinationSysName(String destinationSysName) {
        this.destinationSysName = destinationSysName;
    }

    public boolean getUseCustomerId() {
        return useCustomerId;
    }

    public void setUseCustomerId(boolean useCustomerId) {
        this.useCustomerId = useCustomerId;
    }

    public boolean getIsEntityMatch() {
        return isEntityMatch;
    }

    public void setIsEntityMatch(boolean isEntityMatch) {
        this.isEntityMatch = isEntityMatch;
    }

    public boolean getShouldDefaultPopulateIds() {
        return shouldDefaultPopulateIds;
    }

    public void setShouldDefaultPopulateIds(boolean shouldDefaultPopulateIds) {
        this.shouldDefaultPopulateIds = shouldDefaultPopulateIds;
    }

    public static class DeltaCampaignLaunchSparkContextBuilder {

        private Tenant tenant;
        private String playName;
        private String playLaunchId;
        private PlayLaunch playLaunch;
        private Play play;
        private RatingEngine ratingEngine;
        private MetadataSegment segment;
        private long launchTimestampMillis;
        private String ratingId;
        private RatingModel publishedIteration;
        private List<String> accountColsRecIncluded;
        private List<String> accountColsRecNotIncludedStd;
        private List<String> accountColsRecNotIncludedNonStd;
        private List<String> contactCols;
        private String dataDbDriver;
        private String dataDbUrl;
        private String dataDbUser;
        private String dataDbPassword;
        private String saltHint;
        private String encryptionKey;

        public DeltaCampaignLaunchSparkContextBuilder tenant(Tenant tenant) {
            this.tenant = tenant;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder playName(String playName) {
            this.playName = playName;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder playLaunchId(String playLaunchId) {
            this.playLaunchId = playLaunchId;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder playLaunch(PlayLaunch playLaunch) {
            this.playLaunch = playLaunch;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder play(Play play) {
            this.play = play;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder ratingEngine(RatingEngine ratingEngine) {
            this.ratingEngine = ratingEngine;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder segment(MetadataSegment segment) {
            this.segment = segment;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder launchTimestampMillis(long launchTimestampMillis) {
            this.launchTimestampMillis = launchTimestampMillis;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder ratingId(String ratingId) {
            this.ratingId = ratingId;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder publishedIteration(RatingModel publishedIteration) {
            this.publishedIteration = publishedIteration;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder accountColsRecIncluded(List<String> accountColsRecIncluded) {
            this.accountColsRecIncluded = accountColsRecIncluded;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder accountColsRecNotIncludedStd(
                List<String> accountColsRecNotIncludedStd) {
            this.accountColsRecNotIncludedStd = accountColsRecNotIncludedStd;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder accountColsRecNotIncludedNonStd(
                List<String> accountColsRecNotIncludedNonStd) {
            this.accountColsRecNotIncludedNonStd = accountColsRecNotIncludedNonStd;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder contactCols(List<String> contactCols) {
            this.contactCols = contactCols;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder dataDbDriver(String dataDbDriver) {
            this.dataDbDriver = dataDbDriver;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder dataDbUrl(String dataDbUrl) {
            this.dataDbUrl = dataDbUrl;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder dataDbUser(String dataDbUser) {
            this.dataDbUser = dataDbUser;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder dataDbPassword(String dataDbPassword) {
            this.dataDbPassword = dataDbPassword;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder saltHint(String saltHint) {
            this.saltHint = saltHint;
            return this;
        }

        public DeltaCampaignLaunchSparkContextBuilder encryptionKey(String encryptionKey) {
            this.encryptionKey = encryptionKey;
            return this;
        }

        public DeltaCampaignLaunchSparkContext build() {
            return new DeltaCampaignLaunchSparkContext(this.tenant, this.playName, this.playLaunchId, this.playLaunch,
                    this.play, this.ratingEngine, this.segment, this.launchTimestampMillis, this.ratingId,
                    this.publishedIteration, this.accountColsRecIncluded, this.accountColsRecNotIncludedStd,
                    this.accountColsRecNotIncludedNonStd, this.contactCols, this.dataDbDriver, this.dataDbUrl,
                    this.dataDbUser, this.dataDbPassword, this.saltHint, this.encryptionKey);
        }
    }

}
