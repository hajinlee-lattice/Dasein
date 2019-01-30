package com.latticeengines.domain.exposed.datacloud.dataflow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DateBucket extends BucketAlgorithm  {
    @JsonIgnore
    private static final Logger log = LoggerFactory.getLogger(DateBucket.class);

    private static final long serialVersionUID = -3698813164354927625L;

    public static final String DEFAULT_LAST_7_DAYS = "Last 7 Days";
    public static final String DEFAULT_LAST_30_DAYS = "Last 30 Days";
    public static final String DEFAULT_LAST_90_DAYS = "Last 90 Days";
    public static final String DEFAULT_LAST_180_DAYS = "Last 180 Days";
    public static final String DEFAULT_EVER = "Ever";

    public static final long oneDayInMilliSeconds = 24 * 60 * 60 * 1000;

    @JsonProperty("last7Days")
    private String last7DaysLabel;

    @JsonProperty("last30Days")
    private String last30DaysLabel;

    @JsonProperty("last90Days")
    private String last90DaysLabel;

    @JsonProperty("last180Days")
    private String last180DaysLabel;

    @JsonProperty("ever")
    private String everLabel;

    // Timestamp (in milliseconds) used as base value from which to evaluation all bucket boundaries.
    @JsonProperty("curTimestamp")
    private long curTimestamp;

    // Timestamps that define the boundaries between buckets.
    @JsonProperty("dateBoundaries")
    private List<Long> dateBoundaries;

    // Number of days prior to the current date that each bucket represents.
    @JsonProperty("dayBoundaries")
    private List<Integer> dayBoundaries;

    public DateBucket() { }

    public DateBucket(long curTimestamp) {
        this.curTimestamp = curTimestamp;
        this.dateBoundaries = defineDateBoundaries(curTimestamp);
        this.dayBoundaries = Arrays.asList(7, 30, 90, 180);
    }

    public String getLast7DaysLabel() {
        return last7DaysLabel;
    }

    public void setLast7DaysLabel(String last7DaysLabel) {
        this.last7DaysLabel = last7DaysLabel;
    }

    public String getLast30DaysLabel() {
        return last30DaysLabel;
    }

    public void setLast30DaysLabel(String last30DaysLabel) {
        this.last30DaysLabel = last30DaysLabel;
    }

    public String getLast90DaysLabel() {
        return last90DaysLabel;
    }

    public void setLast90DaysLabel(String last90DaysLabel) {
        this.last90DaysLabel = last90DaysLabel;
    }

    public String getLast180DaysLabel() {
        return last180DaysLabel;
    }

    public void setLast180DaysLabel(String last180DaysLabel) { this.last180DaysLabel = last180DaysLabel; }

    public String getEverLabel() {
        return everLabel;
    }

    public void setEverLabel(String everLabel) {
        this.everLabel = everLabel;
    }

    public Long getCurTimestamp() { return curTimestamp; }

    public void setCurTimestamp(long curTimestamp) { this.curTimestamp = curTimestamp; }

    public List<Long> getDateBoundaries() { return dateBoundaries; }

    public void setDateBoundaries(List<Long> dateBoundaries) {
        this.dateBoundaries = dateBoundaries;
    }

    public List<Integer> getDayBoundaries() {
        return dayBoundaries;
    }

    public void setDayBoundaries(List<Integer> dayBoundaries) {
        this.dayBoundaries = dayBoundaries;
    }


    @JsonIgnore
    public String getLast7DaysLabelWithDefault() {
        return StringUtils.isBlank(last7DaysLabel) ? DEFAULT_LAST_7_DAYS : last7DaysLabel;
    }

    @JsonIgnore
    public String getLast30DaysLabelWithDefault() {
        return StringUtils.isBlank(last30DaysLabel) ? DEFAULT_LAST_30_DAYS : last30DaysLabel;
    }

    @JsonIgnore
    public String getLast90DaysLabelWithDefault() {
        return StringUtils.isBlank(last90DaysLabel) ? DEFAULT_LAST_90_DAYS : last90DaysLabel;
    }

    @JsonIgnore
    public String getLast180DaysLabelWithDefault() {
        return StringUtils.isBlank(last180DaysLabel) ? DEFAULT_LAST_180_DAYS : last180DaysLabel;
    }

    @JsonIgnore
    public String getEverLabelWithDefault() {
        return StringUtils.isBlank(everLabel) ? DEFAULT_EVER : everLabel;
    }

    @Override
    @JsonIgnore
    public String getAlgorithm() {
        return DATE;
    }

    @Override
    @JsonIgnore
    public List<String> generateLabelsInternal () {
        return Arrays.asList(
                null,
                getLast7DaysLabelWithDefault(),
                getLast30DaysLabelWithDefault(),
                getLast90DaysLabelWithDefault(),
                getLast180DaysLabelWithDefault(),
                getEverLabelWithDefault()
        );
    }

    @JsonIgnore
    @Override
    public BucketType getBucketType() {
        return BucketType.Date;
    }

    // Given the current timestamp, computes the date boundaries to be used for data attribute bucketing.  Since dates
    // use the hard coded boundaries, last week, last month, last quarter, last two quarters, last year, and more than
    // a year, the function to compute the bucket boundaries is the same for all date attributes and does not require
    // profiling the values that date attributes hold.
    @JsonIgnore
    static public List<Long> defineDateBoundaries(long curTimestamp) {
        Instant curTime = Instant.ofEpochMilli(curTimestamp);
        // Truncate the current time to the day boundary in UTC to compute the various boundary points.
        Instant roundedDayTime = curTime.truncatedTo(ChronoUnit.DAYS);
        long roundedDayTimestamp = roundedDayTime.toEpochMilli();

        List<Long> boundaryList = new ArrayList<>();
        // Go back 6 days from the beginning of the current day for the one week bucket.
        boundaryList.add(roundedDayTimestamp - 6 * oneDayInMilliSeconds);
        // Go back 29 days from the beginning of the current day for the 30 day (1 month) bucket.
        boundaryList.add(roundedDayTimestamp - 29 * oneDayInMilliSeconds);
        // Go back 89 days from the beginning of the current day for the 90 day (1 quarter) bucket.
        boundaryList.add(roundedDayTimestamp - 89 * oneDayInMilliSeconds);
        // Go back 179 days from the beginning of the current day for the 180 day (2 quarters) bucket.
        boundaryList.add(roundedDayTimestamp - 179 * oneDayInMilliSeconds);

        if (log.isDebugEnabled()) {
            for (Long bucketTimestamp : boundaryList) {
                log.debug("Bucket timestamp is: " + bucketTimestamp + " or date: "
                        + Instant.ofEpochMilli(bucketTimestamp).toString());
            }
        }
        return boundaryList;
    }
}


