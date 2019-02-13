package com.latticeengines.domain.exposed.period;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;
import com.latticeengines.domain.exposed.util.BusinessCalendarUtils;

public abstract class BusinessCalendarBasedPeriodBuilder extends BasePeriodBuilder
        implements PeriodBuilder {

    private static final long serialVersionUID = -1841334335642979430L;
    private static final int DEFAULT_FIRST_YEAR = 2000;
    protected BusinessCalendar calendar;
    protected int startYear;
    protected LocalDate startDate;

    protected BusinessCalendarBasedPeriodBuilder(BusinessCalendar calendar) {
        this(calendar, DEFAULT_FIRST_YEAR);
    }

    protected BusinessCalendarBasedPeriodBuilder(BusinessCalendar calendar, int startYear) {
        this.calendar = calendar;
        this.startYear = startYear;
        this.startDate = getDefaultFirstDate();
    }

    @Override
    public Pair<LocalDate, LocalDate> getDateRangeOfYear(int businessYear) {
        Pair<LocalDate, LocalDate> dateRange;

        switch (calendar.getMode()) {
            case STARTING_DATE:
                dateRange = BusinessCalendarUtils
                        .parseDateRangeFromStartDate(calendar.getStartingDate(), businessYear);
                break;
            case STARTING_DAY:
                dateRange = BusinessCalendarUtils
                        .parseDateRangeFromStartDay(calendar.getStartingDay(), businessYear);
                break;
            case STANDARD:
            default:
                String msg = "Invalid business calendar mode " + calendar.getMode();
                throw new LedpException(LedpCode.LEDP_40015, msg,
                        new UnsupportedOperationException(msg));
        }

        return dateRange;
    }

    protected int getBusinessYear(LocalDate evalDate, Pair<LocalDate, LocalDate> dateRange) {
        if (evalDate.isBefore(dateRange.getLeft())) {
            return evalDate.getYear() - 1;
        } else if (evalDate.isAfter(dateRange.getRight())) {
            return evalDate.getYear() + 1;
        } else {
            return evalDate.getYear();
        }
    }

    protected int getWeekOffsetInBusiYear(LocalDate evalDate,
            Pair<LocalDate, LocalDate> busiYearDateRange) {
        switch (calendar.getMode()) {
            case STARTING_DATE:
                return Math.min(
                        (int) ChronoUnit.DAYS.between(busiYearDateRange.getLeft(), evalDate) / 7,
                        51);
            case STARTING_DAY:
                return (int) ChronoUnit.DAYS.between(busiYearDateRange.getLeft(), evalDate) / 7;
            case STANDARD:
            default:
                String msg = "Invalid business calendar mode " + calendar.getMode();
                throw new LedpException(LedpCode.LEDP_40015, msg,
                        new UnsupportedOperationException(msg));
        }
    }

    /**
     * STARTING_DATE: weekoffset 0 - 12 STARTING_DAY: weekoffset 0 - (11 or 12
     * or 13)
     */
    protected int getMonthOffsetInBusiYear(int weekOffset) {
        int quarterOffset = Math.min(weekOffset, 51) / 13;
        weekOffset -= 13 * quarterOffset;
        int sum = 0;
        for (int i = 0; i < 2; i++) {
            if (i == calendar.getLongerMonth() - 1) { // longer month 1,2,3
                sum += 5;
            } else {
                sum += 4;
            }
            if (weekOffset < sum) {
                return quarterOffset * 3 + i;
            }
        }
        return quarterOffset * 3 + 2;
    }

    protected int getWeekOffsetInBusiYear(int monthOffset) {
        int quarterOffset = monthOffset / 3;
        monthOffset -= 3 * quarterOffset;
        int weekOffset = quarterOffset * 13;
        for (int i = 0; i < monthOffset; i++) {
            if (i == calendar.getLongerMonth() - 1) { // longer month 1,2,3
                weekOffset += 5;
            } else {
                weekOffset += 4;
            }
        }
        return weekOffset;
    }

    private LocalDate getDefaultFirstDate() {
        return getDateRangeOfYear(startYear).getLeft();
    }

}
