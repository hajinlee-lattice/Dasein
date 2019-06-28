package com.latticeengines.domain.exposed.pls;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
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
    private Tenant tenant;

    @JsonProperty("PlayName")
    private String playName;

    @JsonProperty("PlayLaunchId")
    private String playLaunchId;

    @JsonProperty("PlayLaunch")
    private PlayLaunch playLaunch;

    @JsonProperty("Play")
    private Play play;

    @JsonProperty("LaunchTimestampMillis")
    private long launchTimestampMillis;

    @JsonProperty("RatingId")
    private String ratingId;

    @JsonProperty("PublishedIteration")
    private RatingModel publishedIteration;

    @JsonProperty("SynchronizationDestination")
    private String synchronizationDestination;

    @JsonProperty("DestinationSysType")
    private String destinationSysType;

    @JsonProperty("DestinationOrgId")
    private String destinationOrgId;

    @JsonProperty("SfdcAccountID")
    private String sfdcAccountID;

    public PlayLaunchSparkContext() {
    }

    public PlayLaunchSparkContext(Tenant tenant, String playName, String playLaunchId, PlayLaunch playLaunch, Play play,
            long launchTimestampMillis, String ratingId, RatingModel publishedIteration) {
        super();
        this.joinKey = DEFAULT_JOIN_KEY;
        this.tenant = tenant;
        this.playName = playName;
        this.playLaunchId = playLaunchId;
        this.playLaunch = playLaunch;
        this.play = play;
        this.launchTimestampMillis = launchTimestampMillis;
        this.ratingId = ratingId;
        this.publishedIteration = publishedIteration;
        setSyncDestination(this.playLaunch);
    }

    public String getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(String joinKey) {
        this.joinKey = joinKey;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public String getPlayName() {
        return playName;
    }

    public void setPlayName(String playName) {
        this.playName = playName;
    }

    public String getPlayLaunchId() {
        return playLaunchId;
    }

    public void setPlayLaunchId(String playLaunchId) {
        this.playLaunchId = playLaunchId;
    }

    public PlayLaunch getPlayLaunch() {
        return playLaunch;
    }

    public Play getPlay() {
        return play;
    }

    public long getLaunchTimestampMillis() {
        return launchTimestampMillis;
    }

    public void setLaunchTimestampMillis(long launchTimestampMillis) {
        this.launchTimestampMillis = launchTimestampMillis;
    }

    public RatingModel getPublishedIteration() {
        return publishedIteration;
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

    public String getSfdcAccountID() {
        return this.sfdcAccountID;
    }

    public void setSfdcAccountID(String sfdcAccountID) {
        this.sfdcAccountID = sfdcAccountID;
    }

    public String getRatingId() {
        return ratingId;
    }

    public void setRatingId(String ratingId) {
        this.ratingId = ratingId;
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
        } else {
            throw new RuntimeException(String.format("Destination type %s is not supported yet",
                    playLaunch.getDestinationSysType().name()));
        }

        this.synchronizationDestination = synchronizationDestination;
        if (StringUtils.isNotBlank(playLaunch.getDestinationOrgId())) {
            this.destinationOrgId = playLaunch.getDestinationOrgId();
            this.destinationSysType = destinationSysType;
        }
        if (StringUtils.isNotBlank(playLaunch.getDestinationAccountId())) {
            String destinationAccountId = playLaunch.getDestinationAccountId().trim();
            this.sfdcAccountID = destinationAccountId;
        } else {
            this.sfdcAccountID = null;
        }
    }

}
