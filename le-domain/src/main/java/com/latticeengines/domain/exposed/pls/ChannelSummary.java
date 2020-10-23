package com.latticeengines.domain.exposed.pls;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ChannelSummary {

    @JsonProperty("ExternalSystemName")
    private String externalSystemName;

    @JsonProperty("NextScheduledLaunch")
    private Date nextScheduledLaunch;

    @JsonProperty("ExpirationPeriodString")
    private String expirationPeriodString;

    @JsonProperty("UpdatedBy")
    private String updatedBy;

    @JsonProperty("PlayDisplayName")
    private String playDisplayName;

    @JsonProperty("PlayName")
    private String playName;

    public String getExternalSystemName() {
        return this.externalSystemName;
    }

    public void setExternalSystemName(String externalSystemName) {
        this.externalSystemName = externalSystemName;
    }

    public Date getNextScheduledLaunch() {
        return this.nextScheduledLaunch;
    }

    public void setNextScheduledLaunch(Date nextScheduledLaunch) {
        this.nextScheduledLaunch = nextScheduledLaunch;
    }

    public String getExpirationPeriodString() {
        return this.expirationPeriodString;
    }

    public void setExpirationPeriodString(String expirationPeriodString) {
        this.expirationPeriodString = expirationPeriodString;
    }

    public String getUpdatedBy() {
        return this.updatedBy;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

    public String getPlayDisplayName() {
        return this.playDisplayName;
    }

    public void setPlayDisplayName(String playDisplayName) {
        this.playDisplayName = playDisplayName;
    }

    public String getPlayName() {
        return this.playName;
    }

    public void setPlayName(String playName) {
        this.playName = playName;
    }

    public ChannelSummary() {
    }

    public ChannelSummary(PlayLaunchChannel playLaunchChannel) {
        this.externalSystemName = playLaunchChannel.getLookupIdMap() != null
                ? playLaunchChannel.getLookupIdMap().getExternalSystemName().getDisplayName()
                : null;
        this.nextScheduledLaunch = playLaunchChannel.getNextScheduledLaunch();
        this.expirationPeriodString = playLaunchChannel.getExpirationPeriodString();
        this.updatedBy = playLaunchChannel.getUpdatedBy();
        this.playDisplayName = playLaunchChannel.getPlay() != null ? playLaunchChannel.getPlay().getDisplayName()
                : null;
        this.playName = playLaunchChannel.getPlay() != null ? playLaunchChannel.getPlay().getName() : null;
    }

}
