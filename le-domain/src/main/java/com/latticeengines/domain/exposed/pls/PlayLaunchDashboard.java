package com.latticeengines.domain.exposed.pls;

import java.util.Date;
import java.util.List;
import java.util.Set;

public class PlayLaunchDashboard {

    private Counts totalCounts;

    private List<LaunchSummary> launchSummaries;

    public Counts getTotalCounts() {
        return totalCounts;
    }

    public List<LaunchSummary> getLaunchSummaries() {
        return launchSummaries;
    }

    public void setTotalCounts(Counts totalCounts) {
        this.totalCounts = totalCounts;
    }

    public void setLaunchSummaries(List<LaunchSummary> launchSummaries) {
        this.launchSummaries = launchSummaries;
    }

    public static class LaunchSummary {
        private String playName;

        private String playDisplayName;

        private String launchId;

        private Date launchTime;

        private Counts counts;

        private Set<RuleBucketName> selectedBuckets;

        private LaunchState launchState;

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

        public Counts getCounts() {
            return counts;
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

        public void setCounts(Counts counts) {
            this.counts = counts;
        }

        public void setLaunchState(LaunchState launchState) {
            this.launchState = launchState;
        }

        public Set<RuleBucketName> getSelectedBuckets() {
            return selectedBuckets;
        }

        public void setSelectedBuckets(Set<RuleBucketName> selectedBuckets) {
            this.selectedBuckets = selectedBuckets;
        }
    }

    public static class Counts {

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
