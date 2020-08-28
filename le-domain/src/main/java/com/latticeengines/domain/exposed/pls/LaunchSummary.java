package com.latticeengines.domain.exposed.pls;

import java.util.Date;
import java.util.Set;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.LaunchType;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.Stats;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;

public class LaunchSummary {

    private static final String AUTOMATED_LAUNCH = "Automated launch";

    private static final String SYSTEM_USER = "build-admin@lattice-engines.com";

    private String playName;

    private String playDisplayName;

    private String launchId;

    private Date launchTime;

    private Stats stats;

    private Set<RatingBucketName> selectedBuckets;

    private LaunchState launchState;

    private LaunchType launchType;

    private String uiLaunchState;

    private String destinationOrgId;

    private CDLExternalSystemType destinationSysType;

    private String destinationAccountId;

    private CDLExternalSystemName destinationSysName;

    private String audienceName;

    private String folderName;

    private String channelConfig;

    private String createdBy;

    private DataIntegrationStatusMonitor integrationStatusMonitor;

    public LaunchSummary() {

    }

    public LaunchSummary(PlayLaunch launch) {
        Stats stats = new Stats();
        stats.setSelectedTargets(getCount(launch.getAccountsSelected()));
        stats.setSelectedContacts(getCount(launch.getContactsSelected()));
        // PLS-15540 incremental contacts = contactsAdded + contactsDeleted
        stats.setContactsWithinRecommendations(
                getCount(launch.getContactsAdded()) + getCount(launch.getContactsDeleted()));
        stats.setAccountErrors(getCount(launch.getAccountsErrored()));
        stats.setContactErrors(getCount(launch.getContactsErrored()));
        // PLS-15540 incremental accounts = accountsAdded + accountsDeleted
        long accountsAdded = getCount(launch.getAccountsAdded());
        long accountsDeleted = getCount(launch.getAccountsDeleted());
        long contactsAdded = getCount(launch.getContactsAdded());
        long contactsDeleted = getCount(launch.getContactsDeleted());
        stats.setRecommendationsLaunched(accountsAdded + accountsDeleted);
        stats.setAccountsSuppressed(getCount(launch.getAccountsSuppressed()));
        stats.setContactsSuppressed(getCount(launch.getContactsSuppressed()));
        stats.setAccountsDuplicated(getCount(launch.getAccountsDuplicated()));
        stats.setContactsDuplicated(getCount(launch.getContactsDuplicated()));
        stats.setAccountsAdded(accountsAdded);
        stats.setAccountsDeleted(accountsDeleted);
        stats.setContactsAdded(contactsAdded);
        stats.setContactsDeleted(contactsDeleted);
        this.setStats(stats);
        this.setLaunchId(launch.getLaunchId());
        this.setLaunchState(launch.getLaunchState());
        this.setUiLaunchState(launch.getUILaunchState());
        this.setLaunchTime(launch.getCreated());
        this.setSelectedBuckets(launch.getBucketsToLaunch());
        this.setDestinationOrgId(launch.getDestinationOrgId());
        this.setDestinationSysType(launch.getDestinationSysType());
        this.setDestinationAccountId(launch.getDestinationAccountId());
        this.setAudienceName(launch.getAudienceName());
        this.setFolderName(launch.getFolderName());
        this.setLaunchType(launch.getLaunchType());
        this.setCreatedBy(updateSystemLaunch(launch.getCreatedBy()));
        if (launch.getChannelConfig() != null) {
            this.setChannelConfig(launch.getChannelConfig());
        }
        if (launch.getPlay() != null) {
            this.setPlayName(launch.getPlay().getName());
            this.setPlayDisplayName(launch.getPlay().getDisplayName());
        }
    }

    private String updateSystemLaunch(String createdBy) {
        if (SYSTEM_USER.equals(createdBy)) {
            return AUTOMATED_LAUNCH;
        }
        return createdBy;
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

    public String getUiLaunchState() {
        return uiLaunchState;
    }

    public void setUiLaunchState(String uiLaunchState) {
        this.uiLaunchState = uiLaunchState;
    }

    public LaunchType getLaunchType() {
        return launchType;
    }

    public void setLaunchType(LaunchType launchType) {
        this.launchType = launchType;
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

    public String getFolderName() {
        return folderName;
    }

    public void setFolderName(String folderName) {
        this.folderName = folderName;
    }

    public DataIntegrationStatusMonitor getIntegrationStatusMonitor() {
        return integrationStatusMonitor;
    }

    public void setIntegrationStatusMonitor(DataIntegrationStatusMonitor integrationStatusMonitor) {
        this.integrationStatusMonitor = integrationStatusMonitor;
    }

    public void setChannelConfig(ChannelConfig channelConfig) {
        this.channelConfig = JsonUtils.serialize(channelConfig);
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getCreatedBy() {
        return this.createdBy;
    }

    public ChannelConfig getChannelConfig() {
        ChannelConfig newChannelConfig = null;
        if (channelConfig != null) {
            newChannelConfig = JsonUtils.deserialize(channelConfig, ChannelConfig.class);
        }
        return newChannelConfig;
    }

}
