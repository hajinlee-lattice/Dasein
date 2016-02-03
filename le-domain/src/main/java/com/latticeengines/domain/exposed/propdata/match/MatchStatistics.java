package com.latticeengines.domain.exposed.propdata.match;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MatchStatistics {

    private static Log log = LogFactory.getLog(MatchStatistics.class);

    private Integer rowsRequested;
    private Integer rowsMatched;
    private Long timeElapsedInMsec;
    private Date resultGeneratedAt;
    private List<Integer> columnMatchCount;

    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
    private static Calendar calendar = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));
    private static final String durationFormat = "HH:mm:ss.SSS";
    private static final PeriodFormatter periodFormatter =
            new PeriodFormatterBuilder()
                    .appendHours().appendLiteral(":")
                    .appendMinutes().appendLiteral(":")
                    .appendSeconds().appendLiteral(".")
                    .appendMillis3Digit()
                    .toFormatter();

    static  {
        formatter.setCalendar(calendar);
    }

    @JsonProperty("RowsRequested")
    public Integer getRowsRequested() {
        return rowsRequested;
    }

    @JsonProperty("RowsRequested")
    public void setRowsRequested(Integer rowsRequested) {
        this.rowsRequested = rowsRequested;
    }

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

    @JsonIgnore
    public Date getResultGeneratedAt() {
        return resultGeneratedAt;
    }

    @JsonIgnore
    public void setResultGeneratedAt(Date resultGeneratedAt) {
        this.resultGeneratedAt = resultGeneratedAt;
    }

    @JsonProperty("TimeElapsed")
    private String getReadableTimeElapsed() {
        return timeElapsedInMsec == null ? null : DurationFormatUtils.formatDuration(timeElapsedInMsec, durationFormat);
    }

    @JsonProperty("TimeElapsed")
    private void setReadableTimeElapsed(String timeElapsedInMsec) {
        try {
            this.timeElapsedInMsec = Period.parse(timeElapsedInMsec, periodFormatter)
                    .toStandardDuration().getMillis();
        } catch (Exception e) {
            log.error("Cannot parse string " + timeElapsedInMsec + " into a java duration. " +
                    "It has to be in the format of " + durationFormat, e);
            this.timeElapsedInMsec = null;
        }

    }

    @JsonProperty("ResultGeneratedAt")
    private String getResultGeneratedAtAsString() {
        return resultGeneratedAt == null ? null : formatter.format(resultGeneratedAt);
    }

    @JsonProperty("ResultGeneratedAt")
    private void setResultGeneratedAtByString(String resultGeneratedAt) {
        try {
            this.resultGeneratedAt = formatter.parse(resultGeneratedAt);
        } catch (ParseException e) {
            this.resultGeneratedAt = null;
        }
    }
}

