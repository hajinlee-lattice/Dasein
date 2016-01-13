package com.latticeengines.propdata.core.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class DateRange {

    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
    private static Calendar calendar = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));

    static  {
        formatter.setCalendar(calendar);
    }

    private Date startDate;
    private Date endDate;
    private Long durationInMilliSec;

    public DateRange(Date startDate, Date endDate) {
        constructByDates(startDate, endDate);
    }

    public DateRange(String startDate, String endDate) {
        try {
            constructByDates(formatter.parse(startDate + " 00:00:00 UTC"), formatter.parse(endDate + " 00:00:00 UTC"));
        } catch (ParseException e) {
            throw new IllegalArgumentException("Input strings cannot be parsed to dates.", e);
        }
    }

    private void constructByDates(Date startDate, Date endDate) {
        if (endDate.before(startDate)) {
            throw new IllegalArgumentException(String.format("End date %s is before start date %s",
                    formatter.format(endDate), formatter.format(startDate)));
        }

        setStartDate(startDate);
        setEndDate(endDate);
        setDurationInMilliSec(endDate.getTime() - startDate.getTime());
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public Long getDurationInMilliSec() { return durationInMilliSec; }

    public void setDurationInMilliSec(Long durationInMilliSec) { this.durationInMilliSec = durationInMilliSec; }

    public List<DateRange> splitByNumOfPeriods(int numOfPeriods) {
        if (numOfPeriods <= 0) {
            throw new IllegalArgumentException("Number of periods " + numOfPeriods + " is not positive.");
        }

        if (numOfPeriods > getDurationInMilliSec()) {
            throw new IllegalArgumentException("Number of periods " + numOfPeriods
                    + " is more than the whole duration " + getDurationInMilliSec() + " days.");
        }

        double period = ((double) getDurationInMilliSec()) / numOfPeriods;
        double shift = 0.0;

        Date periodStart = startDate;
        Date periodEnd;
        List<DateRange> ranges = new ArrayList<>();

        for (int i = 0; i < numOfPeriods; i++) {
            if (i == numOfPeriods - 1) {
                periodEnd = endDate;
            } else {
                shift += period;
                periodEnd = shiftByMilliSeconds(startDate, Math.round(shift));
            }

            ranges.add(new DateRange(periodStart, periodEnd));

            periodStart = periodEnd;
        }

        return ranges;
    }

    public List<DateRange> splitByDaysPerPeriod(int daysPerPeriod) {
        if (daysPerPeriod <= 0) {
            throw new IllegalArgumentException("Length of period " + daysPerPeriod + " is not positive.");
        }

        Date periodStart = startDate;
        Date periodEnd = startDate;
        List<DateRange> ranges = new ArrayList<>();

        long milliSecondsPerPeriod = TimeUnit.DAYS.toMillis(daysPerPeriod);

        while (periodEnd.before(endDate)) {
            periodEnd = shiftByMilliSeconds(periodEnd, milliSecondsPerPeriod);
            if (periodEnd.after(endDate)) {
                periodEnd = endDate;
            }
            ranges.add(new DateRange(periodStart, periodEnd));
            periodStart = periodEnd;
        }

        return ranges;
    }

    private Date shiftByMilliSeconds(Date date, long milliseconds) {
        calendar.setTime(new Date(date.getTime() + milliseconds));
        return calendar.getTime();
    }

    @Override
    public String toString() { return  "[" + formatter.format(startDate) + " - " + formatter.format(endDate) + "]"; }

    @Override
    public boolean equals(Object that) {
        if (that instanceof DateRange) {
            DateRange thatRange = (DateRange) that;
            return this.getStartDate().equals(thatRange.getStartDate()) &&
                    this.getEndDate().equals(((DateRange) that).getEndDate());
        } else {
            return false;
        }
    }

}
