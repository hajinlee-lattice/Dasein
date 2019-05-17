package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.Map;

public class PlayLaunchDashboard {

    private Stats cumulativeStats;

    private List<LaunchSummary> launchSummaries;

    private List<Play> uniquePlaysWithLaunches;

    private Map<String, List<LookupIdMap>> uniqueLookupIdMapping;

    public Stats getCumulativeStats() {
        return cumulativeStats;
    }

    public void setCumulativeStats(Stats cumulativeStats) {
        this.cumulativeStats = cumulativeStats;
    }

    public List<LaunchSummary> getLaunchSummaries() {
        return launchSummaries;
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

    public static class Stats {

        private long selectedTargets;

        private long suppressed;

        private long accountErrors;

        private long contactErrors;

        private long recommendationsLaunched;

        private long contactsWithinRecommendations;

        private long accountsDuplicated;

        private long contactsDuplicated;

        public long getSelectedTargets() {
            return selectedTargets;
        }

        public void setSelectedTargets(long selectedTargets) {
            this.selectedTargets = selectedTargets;
        }

        public long getSuppressed() {
            return suppressed;
        }

        public void setSuppressed(long suppressed) {
            this.suppressed = suppressed;
        }

        public long getAccountErrors() {
            return accountErrors;
        }

        public void setAccountErrors(long accountErrors) {
            this.accountErrors = accountErrors;
        }

        public long getContactErrors() {
            return contactErrors;
        }

        public void setContactErrors(long contactErrors) {
            this.contactErrors = contactErrors;
        }

        public long getRecommendationsLaunched() {
            return recommendationsLaunched;
        }

        public void setRecommendationsLaunched(long recommendationsLaunched) {
            this.recommendationsLaunched = recommendationsLaunched;
        }

        public long getContactsWithinRecommendations() {
            return contactsWithinRecommendations;
        }

        public void setContactsWithinRecommendations(long contactsWithinRecommendations) {
            this.contactsWithinRecommendations = contactsWithinRecommendations;
        }

        public long getAccountsDuplicated() {
            return accountsDuplicated;
        }

        public void setAccountsDuplicated(long accountsDuplicated) {
            this.accountsDuplicated = accountsDuplicated;
        }

        public long getContactsDuplicated() {
            return contactsDuplicated;
        }

        public void setContactsDuplicated(long contactsDuplicated) {
            this.contactsDuplicated = contactsDuplicated;
        }
    }
}
