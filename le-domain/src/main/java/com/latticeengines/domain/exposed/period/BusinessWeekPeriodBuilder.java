package com.latticeengines.domain.exposed.period;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

public class BusinessWeekPeriodBuilder extends BusinessCalendarBasedPeriodBuilder
        implements PeriodBuilder {

    private static final long serialVersionUID = -7074172925049815611L;

    public BusinessWeekPeriodBuilder(BusinessCalendar businessCalendar) {
        super(businessCalendar);
    }

    public BusinessWeekPeriodBuilder(BusinessCalendar businessCalendar, int startYear) {
        super(businessCalendar, startYear);
    }

    /*
     * PeriodId starts with 0
     */
    @Override
    public int toPeriodId(String date) {
        LocalDate evalDate = LocalDate.parse(date);
        Pair<LocalDate, LocalDate> busiYearDateRange = getDateRangeOfYear(
                evalDate.getYear());
        int businessYear = getBusinessYear(evalDate, busiYearDateRange);
        if (businessYear != evalDate.getYear()) {
            busiYearDateRange = getDateRangeOfYear(businessYear);
        }

        switch (calendar.getMode()) {
            case STARTING_DATE:
                int weekOffsetInBusiYear = getWeekOffsetInBusiYear(evalDate, busiYearDateRange);
                return 52 * (businessYear - startYear) + weekOffsetInBusiYear;
            case STARTING_DAY:
                int daysOffset = (int) ChronoUnit.DAYS.between(startDate, evalDate);
                if (daysOffset >= 0) {
                    return daysOffset / 7;
                } else {
                    return (daysOffset + 1) / 7 - 1;
                }
            default:
                String msg = "Unknown business calendar mode " + calendar.getMode();
                throw new LedpException(LedpCode.LEDP_40015, msg,
                        new UnsupportedOperationException(msg));
        }
    }

    @Override
    protected LocalDate getStartDate(int period) {
        switch (calendar.getMode()) {
            case STARTING_DATE:
                int busiYear = getBusiYearFromWeekPeriod(period);
                int weekOffsetInBusiYear = getWeekOffsetInBusiYearFromWeekPeriod(period);
                LocalDate startDateInBusiYear = getDateRangeOfYear(busiYear).getLeft();
                return startDateInBusiYear.plusDays(weekOffsetInBusiYear * 7);
            case STARTING_DAY:
                return startDate.plusDays(period * 7);
            default:
                String msg = "Unknown business calendar mode " + calendar.getMode();
                throw new LedpException(LedpCode.LEDP_40015, msg,
                        new UnsupportedOperationException(msg));
        }
    }

    @Override
    protected LocalDate getEndDate(int period) {
        switch (calendar.getMode()) {
            case STARTING_DATE:
                int busiYear = getBusiYearFromWeekPeriod(period);
                int weekOffsetInBusiYear = getWeekOffsetInBusiYearFromWeekPeriod(period);
                LocalDate startDateInBusiYear = getDateRangeOfYear(busiYear).getLeft();
                if (weekOffsetInBusiYear == 51) {
                    // First day of next business year - 1
                    return getDateRangeOfYear(busiYear + 1).getLeft().minusDays(1);
                } else {
                    return startDateInBusiYear.plusDays(weekOffsetInBusiYear * 7 + 6);
                }
            case STARTING_DAY:
                return startDate.plusDays(period * 7 + 6);
            default:
                String msg = "Unknown business calendar mode " + calendar.getMode();
                throw new LedpException(LedpCode.LEDP_40015, msg,
                        new UnsupportedOperationException(msg));
        }
    }

    private int getBusiYearFromWeekPeriod(int period) {
        if (period >= 0) {
            return startYear + period / 52;
        } else {
            return startYear + (period + 1) / 52 - 1;
        }
    }

    private int getWeekOffsetInBusiYearFromWeekPeriod(int period) {
        if (period >= 0) {
            return period % 52;
        } else {
            return (period % 52 + 52) % 52;
        }
    }
}
