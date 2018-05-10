package com.latticeengines.domain.exposed.pls;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;

public class PlayLaunchDashboard {

    private Stats cumulativeStats;

    private List<LaunchSummary> launchSummaries;

    private List<Play> uniquePlaysWithLaunches;

    private Map<String, List<LookupIdMap>> uniqueLookupIdMapping;

    public Stats getCumulativeStats() {
        return cumulativeStats;
    }

    public List<LaunchSummary> getLaunchSummaries() {
        return launchSummaries;
    }

    public void setCumulativeStats(Stats cumulativeStats) {
        this.cumulativeStats = cumulativeStats;
    }

    public void setLaunchSummaries(List<LaunchSummary> launchSummaries) {
        this.launchSummaries = launchSummaries;
    }

    public List<Play> getUniquePlaysWithLaunches() {
        return uniquePlaysWithLaunches;
    }

    public void setUniquePlaysWithLaunches(List<Play> uniquePlaysWithLaunches) {
        this.uniquePlaysWithLaunches = uniquePlaysWithLaunches;
    }

    public Map<String, List<LookupIdMap>> getUniqueLookupIdMapping() {
        return uniqueLookupIdMapping;
    }

    public void setUniqueLookupIdMapping(Map<String, List<LookupIdMap>> uniqueLookupIdMapping) {
        this.uniqueLookupIdMapping = uniqueLookupIdMapping;
    }

    public static class LaunchSummary {
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

        public String getPlayName() {
            return playName;
        }

        public String getPlayDisplayName() {
            return playDisplayName;
        }

        public String getLaunchId() {
            return launchId;
        }

        public Date getLaunchTime() {
            return launchTime;
        }

        public Stats getStats() {
            return stats;
        }

        public LaunchState getLaunchState() {
            return launchState;
        }

        public void setPlayName(String playName) {
            this.playName = playName;
        }

        public void setPlayDisplayName(String playDisplayName) {
            this.playDisplayName = playDisplayName;
        }

        public void setLaunchId(String launchId) {
            this.launchId = launchId;
        }

        public void setLaunchTime(Date launchTime) {
            this.launchTime = launchTime;
        }

        public void setStats(Stats stats) {
            this.stats = stats;
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
    }

    public static class Stats {

        private long selectedTargets;

        private long suppressed;

        private long errors;

        private long recommendationsLaunched;

        private long contactsWithinRecommendations;

        public long getSelectedTargets() {
            return selectedTargets;
        }

        public long getSuppressed() {
            return suppressed;
        }

        public long getErrors() {
            return errors;
        }

        public long getRecommendationsLaunched() {
            return recommendationsLaunched;
        }

        public long getContactsWithinRecommendations() {
            return contactsWithinRecommendations;
        }

        public void setSelectedTargets(long selectedTargets) {
            this.selectedTargets = selectedTargets;
        }

        public void setSuppressed(long suppressed) {
            this.suppressed = suppressed;
        }

        public void setErrors(long errors) {
            this.errors = errors;
        }

        public void setRecommendationsLaunched(long recommendationsLaunched) {
            this.recommendationsLaunched = recommendationsLaunched;
        }

        public void setContactsWithinRecommendations(long contactsWithinRecommendations) {
            this.contactsWithinRecommendations = contactsWithinRecommendations;
        }
    }
}
