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
public class PlayLaunchSparkContext implements Serializable {

    private static final long serialVersionUID = -7900616173952618333L;

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

    @JsonProperty("DestinationOrgName")
    private String destinationOrgName;

    @JsonProperty("LaunchSystemName")
    private String launchSystemName;

    @JsonProperty("SfdcAccountID")
    private String sfdcAccountID;

    @JsonProperty("AccountColsRecIncluded")
    private List<String> accountColsRecIncluded;

    @JsonProperty("AccountColsRecNotIncludedStd")
    private List<String> accountColsRecNotIncludedStd;

    @JsonProperty("AccountColsRecNotIncludedNonStd")
    private List<String> accountColsRecNotIncludedNonStd;

    @JsonProperty("ContactCols")
    private List<String> contactCols;

    public PlayLaunchSparkContext() {
    }

    public PlayLaunchSparkContext(Tenant tenant, String playName, String playLaunchId, PlayLaunch playLaunch, Play play,
            RatingEngine ratingEngine, MetadataSegment segment, long launchTimestampMillis, String ratingId,
            RatingModel publishedIteration, List<String> accountColsRecIncluded,
            List<String> accountColsRecNotIncludedStd, List<String> accountColsRecNotIncludedNonStd,
            List<String> contactCols) {
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

    public String getLaunchSystemName() {
        return this.launchSystemName;
    }

    public void setLaunchSystemName(String launchSystemName) {
        this.launchSystemName = launchSystemName;
    }

    public String getSfdcAccountID() {
        return this.sfdcAccountID;
    }

    public void setSfdcAccountID(String sfdcAccountID) {
        this.sfdcAccountID = sfdcAccountID;
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
            this.destinationOrgName = playLaunch.getDestinationOrgName();
            this.destinationSysType = destinationSysType;
        }
        if (StringUtils.isNotBlank(playLaunch.getDestinationAccountId())) {
            String destinationAccountId = playLaunch.getDestinationAccountId().trim();
            this.sfdcAccountID = destinationAccountId;
        } else {
            this.sfdcAccountID = null;
        }
    }

    public static class PlayLaunchSparkContextBuilder {

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

        public PlayLaunchSparkContextBuilder tenant(Tenant tenant) {
            this.tenant = tenant;
            return this;
        }

        public PlayLaunchSparkContextBuilder playName(String playName) {
            this.playName = playName;
            return this;
        }

        public PlayLaunchSparkContextBuilder playLaunchId(String playLaunchId) {
            this.playLaunchId = playLaunchId;
            return this;
        }

        public PlayLaunchSparkContextBuilder playLaunch(PlayLaunch playLaunch) {
            this.playLaunch = playLaunch;
            return this;
        }

        public PlayLaunchSparkContextBuilder play(Play play) {
            this.play = play;
            return this;
        }

        public PlayLaunchSparkContextBuilder ratingEngine(RatingEngine ratingEngine) {
            this.ratingEngine = ratingEngine;
            return this;
        }

        public PlayLaunchSparkContextBuilder segment(MetadataSegment segment) {
            this.segment = segment;
            return this;
        }

        public PlayLaunchSparkContextBuilder launchTimestampMillis(long launchTimestampMillis) {
            this.launchTimestampMillis = launchTimestampMillis;
            return this;
        }

        public PlayLaunchSparkContextBuilder ratingId(String ratingId) {
            this.ratingId = ratingId;
            return this;
        }

        public PlayLaunchSparkContextBuilder publishedIteration(RatingModel publishedIteration) {
            this.publishedIteration = publishedIteration;
            return this;
        }

        public PlayLaunchSparkContextBuilder accountColsRecIncluded(List<String> accountColsRecIncluded) {
            this.accountColsRecIncluded = accountColsRecIncluded;
            return this;
        }

        public PlayLaunchSparkContextBuilder accountColsRecNotIncludedStd(List<String> accountColsRecNotIncludedStd) {
            this.accountColsRecNotIncludedStd = accountColsRecNotIncludedStd;
            return this;
        }

        public PlayLaunchSparkContextBuilder accountColsRecNotIncludedNonStd(
                List<String> accountColsRecNotIncludedNonStd) {
            this.accountColsRecNotIncludedNonStd = accountColsRecNotIncludedNonStd;
            return this;
        }

        public PlayLaunchSparkContextBuilder contactCols(List<String> contactCols) {
            this.contactCols = contactCols;
            return this;
        }

        public PlayLaunchSparkContext build() {
            return new PlayLaunchSparkContext(this.tenant, this.playName, this.playLaunchId, this.playLaunch, this.play,
                    this.ratingEngine, this.segment, this.launchTimestampMillis, this.ratingId, this.publishedIteration,
                    this.accountColsRecIncluded, this.accountColsRecNotIncludedStd,
                    this.accountColsRecNotIncludedNonStd, this.contactCols);
        }
    }

}
