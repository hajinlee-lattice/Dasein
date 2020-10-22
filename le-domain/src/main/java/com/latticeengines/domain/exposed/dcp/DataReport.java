package com.latticeengines.domain.exposed.dcp;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
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

    @JsonProperty("basicStats")
    private BasicStats basicStats;

    @JsonProperty("inputPresenceReport")
    private InputPresenceReport inputPresenceReport;

    @JsonProperty("geoDistributionReport")
    private GeoDistributionReport geoDistributionReport;

    @JsonProperty("matchToDunsReport")
    private MatchToDUNSReport matchToDUNSReport;

    @JsonProperty("duplicationReport")
    private DuplicationReport duplicationReport;

    @JsonProperty("refreshTimestamp")
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

    public DataReport combineReport(DataReport dataReport) {
        Preconditions.checkNotNull(this.basicStats, "basic stats shouldn't be null");
        Preconditions.checkNotNull(this.basicStats.getSuccessCnt(), "successful count shouldn't be null");
        Preconditions.checkNotNull(dataReport.getBasicStats(), "basic stats shouldn't be null");
        Preconditions.checkNotNull(dataReport.getBasicStats().getSuccessCnt(), "count shouldn't be null");
        Long totalCnt1 = this.basicStats.getSuccessCnt();
        Long totalCnt2 = dataReport.getBasicStats().getSuccessCnt();
        this.basicStats.combineBasicStats(dataReport.getBasicStats());
        this.inputPresenceReport.combineInputPresenceReport(dataReport.getInputPresenceReport(), totalCnt1, totalCnt2);
        this.matchToDUNSReport.combineMatchToDUNSReport(dataReport.getMatchToDUNSReport(), totalCnt1, totalCnt2);
        this.geoDistributionReport.combineGeoDistributionReport(dataReport.getGeoDistributionReport(), totalCnt1,
                totalCnt2);
        return this;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class BasicStats {

        @JsonProperty("totalSubmitted")
        private Long totalSubmitted;

        @JsonProperty("successCnt")
        private Long successCnt;

        @JsonProperty("errorCnt")
        private Long errorCnt;

        @JsonProperty("matchedCnt")
        private Long matchedCnt;

        @JsonProperty("pendingReviewCnt")
        private Long pendingReviewCnt;

        @JsonProperty("unmatchedCnt")
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

        public void combineBasicStats(BasicStats basicStats) {
            if (basicStats == null) {
                return ;
            }
            this.totalSubmitted += basicStats.getTotalSubmitted() == null ? 0L : basicStats.getTotalSubmitted();
            this.successCnt += basicStats.getSuccessCnt() == null ? 0L : basicStats.getSuccessCnt();
            this.errorCnt += basicStats.getErrorCnt() == null ? 0L : basicStats.getErrorCnt();
            this.matchedCnt += basicStats.getMatchedCnt() == null ? 0L : basicStats.getMatchedCnt();
            this.pendingReviewCnt += basicStats.getPendingReviewCnt() == null ? 0L : basicStats.getPendingReviewCnt();
            this.unmatchedCnt += basicStats.getUnmatchedCnt() == null ? 0L : basicStats.getUnmatchedCnt();
        }
    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class InputPresenceReport {

        private Map<String, PresenceItem> presenceMap;

        public Map<String, PresenceItem> getPresenceMap() {
            return presenceMap;
        }

        public void setPresenceMap(Map<String, PresenceItem> presenceMap) {
            this.presenceMap = presenceMap;
        }

        @JsonProperty("inputPresenceList")
        public List<PresenceItem> getPresenceList() {
            if (MapUtils.isNotEmpty(presenceMap)) {
                List<PresenceItem> presenceList = new ArrayList<>(presenceMap.values());
                presenceList.sort(Comparator.comparing(PresenceItem::getField));
                return presenceList;
            } else {
                return Collections.emptyList();
            }
        }

        @JsonProperty("inputPresenceList")
        public void setPresenceList(List<PresenceItem> presenceList) {
            if (CollectionUtils.isNotEmpty(presenceList)) {
                presenceMap = presenceList.stream().collect(Collectors.toMap(PresenceItem::getField, item -> item));
            }
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
            presenceItem.setField(field);
            presenceItem.setCount(presenceCnt);
            if (totalCnt == 0L) {
                presenceItem.setRate(getScaledDouble(0.0));
            } else {
                presenceItem.setRate(getScaledDouble((presenceCnt.doubleValue() / totalCnt.doubleValue()) * 100));
            }
            presenceMap.put(field, presenceItem);
        }

        /** totalCount1 represents the successful count in this report,
         * totalCount2 represents the successful count in passing report
         * pass these parameters as the rate is 0 will lose these data
        */
        @JsonIgnore
        public void combineInputPresenceReport(InputPresenceReport inputPresenceReport, Long totalCount1,
                                                         Long totalCount2) {
            if (presenceMap == null) {
                presenceMap = new HashMap<>();
            }
            if (inputPresenceReport == null) {
                return ;
            }
            List<PresenceItem> items = inputPresenceReport.getPresenceList();
            if (CollectionUtils.isNotEmpty(items)) {
                items.forEach(presenceItem -> {
                    String field = presenceItem.getField();
                    PresenceItem oldItem = presenceMap.get(field);
                    if (oldItem == null) {
                        presenceMap.put(field, presenceItem);
                    } else {
                        Long presenceCount = oldItem.getCount() + presenceItem.getCount();
                        oldItem.setCount(presenceCount);
                        Long totalCount = totalCount1 + totalCount2;
                        if (totalCount == 0L) {
                            oldItem.setRate(getScaledDouble(0.0));
                        } else {
                            oldItem.setRate(getScaledDouble((presenceCount.doubleValue() / totalCount.doubleValue()) * 100));
                        }
                    }
                });
            }
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
        public static class PresenceItem {

            @JsonProperty("field")
            private String field;

            @JsonProperty("count")
            private Long count;

            @JsonProperty("rate")
            private Double rate;

            public String getField() {
                return field;
            }

            public void setField(String field) {
                this.field = field;
            }

            public Long getCount() {
                return count;
            }

            public void setCount(Long count) {
                this.count = count;
            }

            public Double getRate() {
                return rate;
            }

            public void setRate(Double rate) {
                this.rate = rate;
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class GeoDistributionReport {

        private Map<String, GeographicalItem> geographicalDistributionMap;

        public Map<String, GeographicalItem> getGeographicalDistributionMap() {
            return geographicalDistributionMap;
        }

        public void setGeographicalDistributionMap(Map<String, GeographicalItem> geographicalDistributionMap) {
            this.geographicalDistributionMap = geographicalDistributionMap;
        }

        @JsonProperty("geoDistributionList")
        public List<GeographicalItem> getGeographicalDistributionList() {
            if (MapUtils.isNotEmpty(geographicalDistributionMap)) {
                List<GeographicalItem> geoDistributionList = new ArrayList<>(geographicalDistributionMap.values());
                geoDistributionList.sort(Comparator.comparing(GeographicalItem::getGeoCode));
                return geoDistributionList;
            } else {
                return Collections.emptyList();
            }
        }

        @JsonProperty("geoDistributionList")
        public void setGeographicalDistributionList(List<GeographicalItem> geographicalItems) {
            if (CollectionUtils.isNotEmpty(geographicalItems)) {
                geographicalDistributionMap =
                        geographicalItems.stream().collect(Collectors.toMap(GeographicalItem::getGeoCode, item -> item));
            }
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
            geographicalItem.setGeoCode(countryCode);
            geographicalItem.setCount(recordCnt);
            if (totalCnt == 0L) {
                geographicalItem.setRate(getScaledDouble(0.0));
            } else {
                geographicalItem.setRate(getScaledDouble((recordCnt.doubleValue() / totalCnt.doubleValue()) * 100));
            }
            geographicalDistributionMap.put(countryCode, geographicalItem);
        }

        /** totalCount1 represents the successful count in this report,
         * totalCount2 represents the successful count in passing report
         * pass these parameters as the rate is 0 will lose these data
         */
        @JsonIgnore
        public void combineGeoDistributionReport(GeoDistributionReport geoDistributionReport,
                                                                  Long totalCount1,
                                                                  Long totalCount2) {
            if (geographicalDistributionMap == null) {
                geographicalDistributionMap = new HashMap<>();
            }
            if (geoDistributionReport == null) {
                return ;
            }
            List<GeographicalItem> items = geoDistributionReport.getGeographicalDistributionList();
            if (CollectionUtils.isNotEmpty(items)) {
                items.forEach(geographicalItem -> {
                    String geoCode = geographicalItem.getGeoCode();
                    GeographicalItem oldItem = geographicalDistributionMap.get(geoCode);
                    if (oldItem == null) {
                        geographicalDistributionMap.put(geoCode, geographicalItem);
                    } else {
                        Long presenceCount = oldItem.getCount() + geographicalItem.getCount();
                        oldItem.setCount(presenceCount);
                        Long totalCount = totalCount1 + totalCount2;
                        if (totalCount == 0L) {
                            oldItem.setRate(getScaledDouble(0.0));
                        } else {
                            oldItem.setRate(getScaledDouble((presenceCount.doubleValue() / totalCount.doubleValue()) * 100));
                        }
                    }
                });
            }
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
        public static class GeographicalItem {

            @JsonProperty("geoCode")
            private String geoCode;

            @JsonProperty("count")
            private Long count;

            @JsonProperty("rate")
            private Double rate;

            public String getGeoCode() {
                return geoCode;
            }

            public void setGeoCode(String geoCode) {
                this.geoCode = geoCode;
            }

            public Long getCount() {
                return count;
            }

            public void setCount(Long count) {
                this.count = count;
            }

            public Double getRate() {
                return rate;
            }

            public void setRate(Double rate) {
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

        @JsonProperty("noMatchCnt")
        private Long noMatchCnt;

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

        @JsonProperty("confidenceRateList")
        public List<ConfidenceItem> getConfidenceRateList() {
            if (MapUtils.isNotEmpty(confidenceRateMap)) {
                List<ConfidenceItem> confidenceRateList = new ArrayList<>(confidenceRateMap.values());
                confidenceRateList.sort(Comparator.comparing(ConfidenceItem::getConfidenceCode));
                return confidenceRateList;
            } else {
                return Collections.emptyList();
            }
        }

        @JsonProperty("confidenceRateList")
        public void setConfidenceRateList(List<ConfidenceItem> confidenceItems) {
            if (CollectionUtils.isNotEmpty(confidenceItems)) {
                confidenceRateMap =
                        confidenceItems.stream().collect(Collectors.toMap(ConfidenceItem::getConfidenceCode, item -> item));
            }
        }

        @JsonIgnore
        public void addConfidenceItem(Integer confidenceCode, Long recordCnt, Long totalCnt) {
            Preconditions.checkNotNull(confidenceCode);
            Preconditions.checkNotNull(recordCnt);
            Preconditions.checkNotNull(totalCnt);
            Preconditions.checkArgument(totalCnt >= recordCnt);
            Preconditions.checkArgument(confidenceCode >= 0);
            Preconditions.checkArgument(confidenceCode <= 10);
            if (confidenceRateMap == null) {
                confidenceRateMap = new HashMap<>();
            }
            ConfidenceItem confidenceItem = new ConfidenceItem();
            confidenceItem.setConfidenceCode(confidenceCode);
            confidenceItem.setCount(recordCnt);
            if (totalCnt == 0L) {
                confidenceItem.setRate(getScaledDouble(0.0));
            } else {
                confidenceItem.setRate(getScaledDouble((recordCnt.doubleValue() / totalCnt.doubleValue()) * 100));
            }
            ConfidenceItem.Classification classification = confidenceCode < 5 ? ConfidenceItem.Classification.Low :
                    (confidenceCode < 8 ? ConfidenceItem.Classification.Medium : ConfidenceItem.Classification.High);
            confidenceItem.setClassification(classification);
            confidenceRateMap.put(confidenceCode, confidenceItem);
        }

        /** totalCnt1 represents the successful count in this report,
         * totalCnt2 represents the successful count in passing report
         * pass these parameters as the rate is 0 will lose these data
         */
        @JsonIgnore
        public void combineMatchToDUNSReport(MatchToDUNSReport matchToDUNSReport, Long totalCnt1,
                                                          Long totalCnt2) {
            if (confidenceRateMap == null) {
                confidenceRateMap = new HashMap<>();
            }
            if (matchToDUNSReport == null) {
                return ;
            }
            if (this.matched == null) {
                this.matched = 0L;
            }
            this.matched += matchToDUNSReport.getMatched() == null ? 0L : matchToDUNSReport.getMatched();

            if (this.unmatched == null) {
                this.unmatched = 0L;
            }
            this.unmatched += matchToDUNSReport.getUnmatched() == null ? 0L : matchToDUNSReport.getUnmatched();

            if (this.noMatchCnt == null) {
                this.noMatchCnt = 0L;
            }
            this.noMatchCnt += matchToDUNSReport.getNoMatchCnt() == null ? 0L : matchToDUNSReport.getNoMatchCnt();

            List<ConfidenceItem> items = matchToDUNSReport.getConfidenceRateList();
            if (CollectionUtils.isNotEmpty(items)) {
                items.forEach(confidenceItem -> {
                    Integer confidenceCode = confidenceItem.getConfidenceCode();
                    ConfidenceItem oldItem = confidenceRateMap.get(confidenceCode);
                    if (oldItem == null) {
                        confidenceRateMap.put(confidenceCode, confidenceItem);
                    } else {
                        Long confidenceCount = oldItem.getCount() + confidenceItem.getCount();
                        oldItem.setCount(confidenceCount);
                        Long totalCount = totalCnt1 + totalCnt2;
                        if (totalCount == 0L) {
                            oldItem.setRate(getScaledDouble(0.0));
                        } else {
                            oldItem.setRate(getScaledDouble((confidenceCount.doubleValue() / totalCount.doubleValue()) * 100));
                        }
                    }
                });
            }
        }

        @JsonProperty("matchedRate")
        public double getMatchedRate() {
            double matchedCnt = matched != null ? matched.doubleValue() : 0.0;
            double unmatchedCnt = unmatched != null ? unmatched.doubleValue() : 0.0;
            double total = matchedCnt + unmatchedCnt;
            if (total > 0) {
                return getScaledDouble((matchedCnt / total) * 100);
            } else {
                return getScaledDouble(0.0);
            }
        }

        @JsonProperty("unmatchedRate")
        public double getUnmatchedRate() {
            double matchedCnt = matched != null ? matched.doubleValue() : 0.0;
            double unmatchedCnt = unmatched != null ? unmatched.doubleValue() : 0.0;
            double total = matchedCnt + unmatchedCnt;
            if (total > 0) {
                return getScaledDouble((unmatchedCnt / total) * 100);
            } else {
                return getScaledDouble(0.0);
            }
        }

        @JsonProperty("noMatchRate")
        public double getNoMatchRate() {
            double matchedCnt = matched != null ? matched.doubleValue() : 0.0;
            double unmatchedCnt = unmatched != null ? unmatched.doubleValue() : 0.0;
            double total = matchedCnt + unmatchedCnt;
            double noMatch = noMatchCnt != null ? noMatchCnt.doubleValue() : 0.0;
            if (total > 0) {
                return getScaledDouble((noMatch / total) * 100);
            } else {
                return getScaledDouble(0.0);
            }
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
        public static class ConfidenceItem {

            @JsonProperty("confidenceCode")
            private Integer confidenceCode;

            @JsonProperty("count")
            private Long count;

            @JsonProperty("rate")
            private Double rate;

            @JsonProperty("classification")
            private Classification classification;

            public Integer getConfidenceCode() {
                return confidenceCode;
            }

            public void setConfidenceCode(Integer confidenceCode) {
                this.confidenceCode = confidenceCode;
            }

            public Long getCount() {
                return count;
            }

            public void setCount(Long count) {
                this.count = count;
            }

            public Double getRate() {
                return rate;
            }

            public void setRate(Double rate) {
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

        @JsonProperty("uniqueRecords")
        private Long uniqueRecords;

        @JsonProperty("duplicateRecords")
        private Long duplicateRecords;

        @JsonProperty("distinctRecords")
        private Long distinctRecords;

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

        public Long getDistinctRecords() {
            return distinctRecords;
        }

        public void setDistinctRecords(Long distinctRecords) {
            this.distinctRecords = distinctRecords;
        }

        @JsonProperty("uniqueRate")
        public double getUniqueRate() {
            double unique = uniqueRecords != null ? uniqueRecords.doubleValue() : 0.0;
            double duplicate = duplicateRecords != null ? duplicateRecords.doubleValue() : 0.0;
            double total = unique + duplicate;
            if (total > 0) {
                return getScaledDouble((unique / total) * 100);
            } else {
                return 0.0;
            }
        }

        @JsonProperty("duplicateRate")
        public double getDuplicateRate() {
            double uniqueRate = getUniqueRate();
            long duplicate = duplicateRecords != null ? duplicateRecords : 0L;
            if (duplicate > 0) {
                return getScaledDouble(100 - uniqueRate);
            } else {
                return 0.0;
            }
        }
    }

    public static double getScaledDouble(double d) {
        BigDecimal bd = new BigDecimal(d).round(new MathContext(3));
        return bd.doubleValue();
    }
}
