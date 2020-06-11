package com.latticeengines.domain.exposed.dcp;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * JSON definition for Dashboard Report
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataReport {

    @JsonProperty("basic_stats")
    private BasicStats basicStats;

    @JsonProperty("input_presence_report")
    private InputPresenceReport inputPresenceReport;

    @JsonProperty("geo_distribution_report")
    private GeoDistributionReport geoDistributionReport;

    @JsonProperty("match_to_duns_report")
    private MatchToDUNSReport matchToDUNSReport;

    @JsonProperty("duplication_report")
    private DuplicationReport duplicationReport;

    @JsonProperty("refresh_timestamp")
    private Long refreshTimestamp;

    public BasicStats getBasicStats() {
        return basicStats;
    }

    public void setBasicStats(BasicStats basicStats) {
        this.basicStats = basicStats;
    }

    public InputPresenceReport getInputPresenceReport() {
        return inputPresenceReport;
    }

    public void setInputPresenceReport(InputPresenceReport inputPresenceReport) {
        this.inputPresenceReport = inputPresenceReport;
    }

    public GeoDistributionReport getGeoDistributionReport() {
        return geoDistributionReport;
    }

    public void setGeoDistributionReport(GeoDistributionReport geoDistributionReport) {
        this.geoDistributionReport = geoDistributionReport;
    }

    public MatchToDUNSReport getMatchToDUNSReport() {
        return matchToDUNSReport;
    }

    public void setMatchToDUNSReport(MatchToDUNSReport matchToDUNSReport) {
        this.matchToDUNSReport = matchToDUNSReport;
    }

    public DuplicationReport getDuplicationReport() {
        return duplicationReport;
    }

    public void setDuplicationReport(DuplicationReport duplicationReport) {
        this.duplicationReport = duplicationReport;
    }

    public Long getRefreshTimestamp() {
        return refreshTimestamp;
    }

    public void setRefreshTimestamp(Long refreshTimestamp) {
        this.refreshTimestamp = refreshTimestamp;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class BasicStats {

        @JsonProperty("total_submitted")
        private Long totalSubmitted;

        @JsonProperty("success_cnt")
        private Long successCnt;

        @JsonProperty("error_cnt")
        private Long errorCnt;

        @JsonProperty("matched_cnt")
        private Long matchedCnt;

        @JsonProperty("pending_review_cnt")
        private Long pendingReviewCnt;

        @JsonProperty("unmatched_cnt")
        private Long unmatchedCnt;

        public Long getTotalSubmitted() {
            return totalSubmitted;
        }

        public void setTotalSubmitted(Long totalSubmitted) {
            this.totalSubmitted = totalSubmitted;
        }

        public Long getSuccessCnt() {
            return successCnt;
        }

        public void setSuccessCnt(Long successCnt) {
            this.successCnt = successCnt;
        }

        public Long getErrorCnt() {
            return errorCnt;
        }

        public void setErrorCnt(Long errorCnt) {
            this.errorCnt = errorCnt;
        }

        public Long getMatchedCnt() {
            return matchedCnt;
        }

        public void setMatchedCnt(Long matchedCnt) {
            this.matchedCnt = matchedCnt;
        }

        public Long getPendingReviewCnt() {
            return pendingReviewCnt;
        }

        public void setPendingReviewCnt(Long pendingReviewCnt) {
            this.pendingReviewCnt = pendingReviewCnt;
        }

        public Long getUnmatchedCnt() {
            return unmatchedCnt;
        }

        public void setUnmatchedCnt(Long unmatchedCnt) {
            this.unmatchedCnt = unmatchedCnt;
        }
    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class InputPresenceReport {

        @JsonProperty("input_presence_map")
        private Map<String, PresenceItem> presenceMap;

        public Map<String, PresenceItem> getPresenceMap() {
            return presenceMap;
        }

        public void setPresenceMap(Map<String, PresenceItem> presenceMap) {
            this.presenceMap = presenceMap;
        }

        @JsonIgnore
        public void addPresence(String field, Long presenceCnt, Long totalCnt) {
            Preconditions.checkArgument(StringUtils.isNotEmpty(field));
            Preconditions.checkNotNull(presenceCnt);
            Preconditions.checkNotNull(totalCnt);
            Preconditions.checkArgument(totalCnt >= presenceCnt);
            if (presenceMap == null) {
                presenceMap = new HashMap<>();
            }
            PresenceItem presenceItem = new PresenceItem();
            presenceItem.setCount(presenceCnt);
            if (totalCnt == 0L) {
                presenceItem.setRate(0);
            } else {
                presenceItem.setRate((int)Math.round((presenceCnt.doubleValue() / totalCnt.doubleValue()) * 100));
            }
            presenceMap.put(field, presenceItem);
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
        public static class PresenceItem {

            @JsonProperty("count")
            private Long count;

            @JsonProperty("rate")
            private Integer rate;

            public Long getCount() {
                return count;
            }

            public void setCount(Long count) {
                this.count = count;
            }

            public Integer getRate() {
                return rate;
            }

            public void setRate(Integer rate) {
                this.rate = rate;
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class GeoDistributionReport {

        @JsonProperty("geo_distribution_map")
        private Map<String, GeographicalItem> geographicalDistributionMap;

        public Map<String, GeographicalItem> getGeographicalDistributionMap() {
            return geographicalDistributionMap;
        }

        public void setGeographicalDistributionMap(Map<String, GeographicalItem> geographicalDistributionMap) {
            this.geographicalDistributionMap = geographicalDistributionMap;
        }

        @JsonIgnore
        public void addGeoDistribution(String countryCode, Long recordCnt, Long totalCnt) {
            Preconditions.checkArgument(StringUtils.isNotEmpty(countryCode));
            Preconditions.checkNotNull(recordCnt);
            Preconditions.checkNotNull(totalCnt);
            Preconditions.checkArgument(totalCnt >= recordCnt);
            if (geographicalDistributionMap == null) {
                geographicalDistributionMap = new HashMap<>();
            }
            GeographicalItem geographicalItem = new GeographicalItem();
            geographicalItem.setCount(recordCnt);
            if (totalCnt == 0L) {
                geographicalItem.setRate(0);
            } else {
                geographicalItem.setRate((int)Math.round((recordCnt.doubleValue() / totalCnt.doubleValue()) * 100));
            }
            geographicalDistributionMap.put(countryCode, geographicalItem);
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
        public static class GeographicalItem {

            @JsonProperty("count")
            private Long count;

            @JsonProperty("rate")
            private Integer rate;

            public Long getCount() {
                return count;
            }

            public void setCount(Long count) {
                this.count = count;
            }

            public Integer getRate() {
                return rate;
            }

            public void setRate(Integer rate) {
                this.rate = rate;
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class MatchToDUNSReport {

        @JsonProperty("matched")
        private Long matched;

        @JsonProperty("unmatched")
        private Long unmatched;

        @JsonProperty("no_match_cnt")
        private Long noMatchCnt;

        @JsonProperty("confidence_rate_map")
        private Map<Integer, ConfidenceItem> confidenceRateMap;

        public Long getMatched() {
            return matched;
        }

        public void setMatched(Long matched) {
            this.matched = matched;
        }

        public Long getUnmatched() {
            return unmatched;
        }

        public void setUnmatched(Long unmatched) {
            this.unmatched = unmatched;
        }

        public Long getNoMatchCnt() {
            return noMatchCnt;
        }

        public void setNoMatchCnt(Long noMatchCnt) {
            this.noMatchCnt = noMatchCnt;
        }

        public Map<Integer, ConfidenceItem> getConfidenceRateMap() {
            return confidenceRateMap;
        }

        public void setConfidenceRateMap(Map<Integer, ConfidenceItem> confidenceRateMap) {
            this.confidenceRateMap = confidenceRateMap;
        }

        @JsonIgnore
        public void addConfidenceItem(Integer confidenceCode, Long recordCnt, Long totalCnt) {
            Preconditions.checkNotNull(confidenceCode);
            Preconditions.checkNotNull(recordCnt);
            Preconditions.checkNotNull(totalCnt);
            Preconditions.checkArgument(totalCnt >= recordCnt);
            Preconditions.checkArgument(confidenceCode > 0);
            Preconditions.checkArgument(confidenceCode <= 10);
            if (confidenceRateMap == null) {
                confidenceRateMap = new HashMap<>();
            }
            ConfidenceItem confidenceItem = new ConfidenceItem();
            confidenceItem.setCount(recordCnt);
            if (totalCnt == 0L) {
                confidenceItem.setRate(0);
            } else {
                confidenceItem.setRate((int)Math.round((recordCnt.doubleValue() / totalCnt.doubleValue()) * 100));
            }
            ConfidenceItem.Classification classification = confidenceCode < 5 ? ConfidenceItem.Classification.Low :
                    (confidenceCode < 8 ? ConfidenceItem.Classification.Medium : ConfidenceItem.Classification.High);
            confidenceItem.setClassification(classification);
            confidenceRateMap.put(confidenceCode, confidenceItem);
        }

        @JsonProperty("matched_rate")
        public int getMatchedRate() {
            double matchedCnt = matched != null ? matched.doubleValue() : 0.0;
            double unmatchedCnt = unmatched != null ? unmatched.doubleValue() : 0.0;
            double total = matchedCnt + unmatchedCnt;
            if (total > 0) {
                return (int) Math.round((matchedCnt / total) * 100);
            } else {
                return 0;
            }
        }

        @JsonProperty("unmatched_rate")
        public int getUnmatchedRate() {
            double matchedCnt = matched != null ? matched.doubleValue() : 0.0;
            double unmatchedCnt = unmatched != null ? unmatched.doubleValue() : 0.0;
            double total = matchedCnt + unmatchedCnt;
            if (total > 0) {
                return (int) Math.round((unmatchedCnt / total) * 100);
            } else {
                return 0;
            }
        }

        @JsonProperty("no_match_rate")
        public int getNoMatchRate() {
            double unmatchedCnt = unmatched != null ? unmatched.doubleValue() : 0.0;
            double noMatch = noMatchCnt != null ? noMatchCnt.doubleValue() : 0.0;
            if (unmatchedCnt == 0) {
                return 0;
            } else if (noMatch > unmatchedCnt) {
                return 1;
            } else {
                return (int) Math.round((noMatch / unmatchedCnt) * 100);
            }
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
        public static class ConfidenceItem {

            @JsonProperty("count")
            private Long count;

            @JsonProperty("rate")
            private Integer rate;

            @JsonProperty("classification")
            private Classification classification;

            public Long getCount() {
                return count;
            }

            public void setCount(Long count) {
                this.count = count;
            }

            public Integer getRate() {
                return rate;
            }

            public void setRate(Integer rate) {
                this.rate = rate;
            }

            public Classification getClassification() {
                return classification;
            }

            public void setClassification(Classification classification) {
                this.classification = classification;
            }

            public enum Classification {
                Low, Medium, High
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class DuplicationReport {

        @JsonProperty("unique_records")
        private Long uniqueRecords;

        @JsonProperty("duplicate_records")
        private Long duplicateRecords;

        public Long getUniqueRecords() {
            return uniqueRecords;
        }

        public void setUniqueRecords(Long uniqueRecords) {
            this.uniqueRecords = uniqueRecords;
        }

        public Long getDuplicateRecords() {
            return duplicateRecords;
        }

        public void setDuplicateRecords(Long duplicateRecords) {
            this.duplicateRecords = duplicateRecords;
        }

        @JsonProperty("unique_rate")
        public int getUniqueRate() {
            double unique = uniqueRecords != null ? uniqueRecords.doubleValue() : 0.0;
            double duplicate = duplicateRecords != null ? duplicateRecords.doubleValue() : 0.0;
            double total = unique + duplicate;
            if (total > 0) {
                return (int)Math.round((unique / total) * 100);
            } else {
                return 100;
            }
        }

        @JsonProperty("duplicate_rate")
        public int getDuplicateRate() {
            return 100 - getUniqueRate();
        }
    }
}
