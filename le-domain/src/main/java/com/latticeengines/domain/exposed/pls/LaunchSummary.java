package com.latticeengines.domain.exposed.pls;

import java.util.Date;
import java.util.Set;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.Stats;

public class LaunchSummary {

    private String playName;

    private String playDisplayName;

    private String launchId;

    private Date launchTime;

    private Stats stats;

    private Set<RatingBucketName> selectedBuckets;

    private LaunchState launchState;

    private String destinationOrgId;

    private CDLExternalSystemType destinationSysType;

    private String destinationAccountId;

    private CDLExternalSystemName destinationSysName;

    private String audienceName;

    private DataIntegrationStatusMonitor integrationStatusMonitor;

    public LaunchSummary() {

    }

    public LaunchSummary(PlayLaunch launch) {
        Stats stats = new Stats();
        stats.setSelectedTargets(getCount(launch.getAccountsSelected()));
        stats.setContactsWithinRecommendations(getCount(launch.getContactsLaunched()));
        stats.setAccountErrors(getCount(launch.getAccountsErrored()));
        stats.setContactErrors(getCount(launch.getContactsErrored()));
        stats.setRecommendationsLaunched(getCount(launch.getAccountsLaunched()));
        stats.setSuppressed(getCount(launch.getAccountsSuppressed()));
        stats.setAccountsDuplicated(getCount(launch.getAccountsDuplicated()));
        stats.setContactsDuplicated(getCount(launch.getContactsDuplicated()));

        this.setStats(stats);
        this.setLaunchId(launch.getLaunchId());
        this.setLaunchState(launch.getLaunchState());
        this.setLaunchTime(launch.getCreated());
        this.setSelectedBuckets(launch.getBucketsToLaunch());
        this.setDestinationOrgId(launch.getDestinationOrgId());
        this.setDestinationSysType(launch.getDestinationSysType());
        this.setDestinationAccountId(launch.getDestinationAccountId());
        this.setAudienceName(launch.getAudienceName());
        if (launch.getPlay() != null) {
            this.setPlayName(launch.getPlay().getName());
            this.setPlayDisplayName(launch.getPlay().getDisplayName());
        }
    }

    private long getCount(Long count) {
        return count == null ? 0L : count;
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

    public String getLaunchId() {
        return launchId;
    }

    public void setLaunchId(String launchId) {
        this.launchId = launchId;
    }

    public Date getLaunchTime() {
        return launchTime;
    }

    public void setLaunchTime(Date launchTime) {
        this.launchTime = launchTime;
    }

    public Stats getStats() {
        return stats;
    }

    public void setStats(Stats stats) {
        this.stats = stats;
    }

    public LaunchState getLaunchState() {
        return launchState;
    }

    public void setLaunchState(LaunchState launchState) {
        this.launchState = launchState;
    }

    public Set<RatingBucketName> getSelectedBuckets() {
        return selectedBuckets;
    }

    public void setSelectedBuckets(Set<RatingBucketName> selectedBuckets) {
        this.selectedBuckets = selectedBuckets;
    }

    public String getDestinationOrgId() {
        return destinationOrgId;
    }

    public void setDestinationOrgId(String destinationOrgId) {
        this.destinationOrgId = destinationOrgId;
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

    public CDLExternalSystemName getDestinationSysName() {
        return destinationSysName;
    }

    public void setDestinationSysName(CDLExternalSystemName destinationSysName) {
        this.destinationSysName = destinationSysName;
    }

    public String getAudienceName() {
        return audienceName;
    }

    public void setAudienceName(String audienceName) {
        this.audienceName = audienceName;
    }

    public DataIntegrationStatusMonitor getIntegrationStatusMonitor() {
        return integrationStatusMonitor;
    }

    public void setIntegrationStatusMonitor(DataIntegrationStatusMonitor integrationStatusMonitor) {
        this.integrationStatusMonitor = integrationStatusMonitor;
    }

}
