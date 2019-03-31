package com.latticeengines.domain.exposed.datacloud.match;

import java.util.List;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.annotation.MetricField;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MatchStatistics {

    private static final String durationFormat = "HH:mm:ss.SSS";
    private static final PeriodFormatter periodFormatter = new PeriodFormatterBuilder()
            .appendHours().appendLiteral(":").appendMinutes().appendLiteral(":").appendSeconds()
            .appendLiteral(".").appendMillis3Digit().toFormatter();
    private static Logger log = LoggerFactory.getLogger(MatchStatistics.class);
    private Integer rowsRequested;
    private Integer rowsMatched;
    private Long timeElapsedInMsec;
    private List<Integer> columnMatchCount;

    private Long orphanedNoMatchCount;
    private Long orphanedUnmatchedAccountIdCount;
    private Long matchedByMatchKeyCount;
    private Long matchedByAccountIdCount;

    //private EntityMatchAggregation entityMatchAggregation;

    @MetricField(name = "RowsRequested", fieldType = MetricField.FieldType.INTEGER)
    @JsonProperty("RowsRequested")
    public Integer getRowsRequested() {
        return rowsRequested;
    }

    @JsonProperty("RowsRequested")
    public void setRowsRequested(Integer rowsRequested) {
        this.rowsRequested = rowsRequested;
    }

    @MetricField(name = "RowsMatched", fieldType = MetricField.FieldType.INTEGER)
    @JsonProperty("RowsMatched")
    public Integer getRowsMatched() {
        return rowsMatched;
    }

    @JsonProperty("RowsMatched")
    public void setRowsMatched(Integer rowsMatched) {
        this.rowsMatched = rowsMatched;
    }

    @JsonIgnore
    public Long getTimeElapsedInMsec() {
        return timeElapsedInMsec;
    }

    @JsonIgnore
    public void setTimeElapsedInMsec(Long timeElapsedInMsec) {
        this.timeElapsedInMsec = timeElapsedInMsec;
    }

    @JsonProperty("ColumnMatchCount")
    public List<Integer> getColumnMatchCount() {
        return columnMatchCount;
    }

    @JsonProperty("ColumnMatchCount")
    public void setColumnMatchCount(List<Integer> columnMatchCount) {
        this.columnMatchCount = columnMatchCount;
    }

    /*
    public EntityMatchAggregation getEntityMatchAggregation() {
        return entityMatchAggregation;
    }

    public void setEntityMatchAggregation(EntityMatchAggregation entityMatchAggregation) {
        this.entityMatchAggregation = entityMatchAggregation;
    }
    */

    public Long getOrphanedNoMatchCount() {
        return orphanedNoMatchCount;
    }

    public void setOrphanedNoMatchCount(Long orphanedNoMatchCount) {
        this.orphanedNoMatchCount = orphanedNoMatchCount;
    }

    public Long getOrphanedUnmatchedAccountIdCount() {
        return orphanedUnmatchedAccountIdCount;
    }

    public void setOrphanedUnmatchedAccountIdCount(Long orphanedUnmatchedAccountIdCount) {
        this.orphanedUnmatchedAccountIdCount = orphanedUnmatchedAccountIdCount;
    }

    public Long getMatchedByMatchKeyCount() {
        return matchedByMatchKeyCount;
    }

    public void setMatchedByMatchKeyCount(Long matchedByMatchKeyCount) {
        this.matchedByMatchKeyCount = matchedByMatchKeyCount;
    }

    public Long getMatchedByAccountIdCount() {
        return matchedByAccountIdCount;
    }

    public void setMatchedByAccountIdCount(Long matchedByAccountIdCount) {
        this.matchedByAccountIdCount = matchedByAccountIdCount;
    }

    @JsonProperty("TimeElapsed")
    private String getReadableTimeElapsed() {
        return timeElapsedInMsec == null ? null
                : DurationFormatUtils.formatDuration(timeElapsedInMsec, durationFormat);
    }

    @JsonProperty("TimeElapsed")
    private void setReadableTimeElapsed(String timeElapsedInMsec) {
        try {
            this.timeElapsedInMsec = Period.parse(timeElapsedInMsec, periodFormatter)
                    .toStandardDuration().getMillis();
        } catch (Exception e) {
            log.error("Cannot parse string " + timeElapsedInMsec + " into a java duration. "
                    + "It has to be in the format of " + durationFormat, e);
            this.timeElapsedInMsec = null;
        }
    }

    @MetricField(name = "TimeElapsed", fieldType = MetricField.FieldType.INTEGER)
    @JsonIgnore
    public Integer getTimeElapsedMetric() {
        return Integer.valueOf(getTimeElapsedInMsec().toString());
    }

    public void printMatchStatistics(String message) {
        log.error("$JAW$ Printing Match Stats: " + message);
        log.error("$JAW$    Stats Rows Matched: " + getRowsMatched());
        log.error("$JAW$    Stats Orphaned No Match: " + getOrphanedNoMatchCount());
        log.error("$JAW$    Stats Orphaned Unmatched Account ID: " + getOrphanedUnmatchedAccountIdCount());
        log.error("$JAW$    Stats Matched By MatchKey: " + getMatchedByMatchKeyCount());
        log.error("$JAW$    Stats Matched By Account ID: " + getMatchedByAccountIdCount());
    }

}
