package com.latticeengines.domain.exposed.period;

import java.time.LocalDate;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

public class BusinessQuarterPeriodBuilder extends BusinessCalendarBasedPeriodBuilder
        implements PeriodBuilder {

    private static final long serialVersionUID = -1476479105311208951L;

    public BusinessQuarterPeriodBuilder(BusinessCalendar businessCalendar) {
        super(businessCalendar);
    }

    public BusinessQuarterPeriodBuilder(BusinessCalendar businessCalendar, int startYear) {
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

        int weekOffsetInBusiYear = getWeekOffsetInBusiYear(evalDate, busiYearDateRange);
        int monthOffsetInBusiYear = getMonthOffsetInBusiYear(weekOffsetInBusiYear);
        return 4 * (businessYear - startYear) + monthOffsetInBusiYear / 3;
    }

    @Override
    protected LocalDate getStartDate(int period) {
        int busiYear = getBusiYearFromQuarterPeriod(period);
        int quarterOffset = getQuarterOffsetInBusiYearFromQuarterPeriod(period);
        return getDateRangeOfYear(busiYear).getLeft().plusDays(13 * 7 * quarterOffset);
    }

    @Override
    protected LocalDate getEndDate(int period) {
        int busiYear = getBusiYearFromQuarterPeriod(period);
        int quarterOffset = getQuarterOffsetInBusiYearFromQuarterPeriod(period);

        if (quarterOffset == 3) {
            // First day of next business year - 1
            return getDateRangeOfYear(busiYear + 1).getLeft().minusDays(1);
        } else {
            int weekOffsetOfNextQuarter = 13 * (quarterOffset + 1);
            // First day of next business quarter - 1
            return getDateRangeOfYear(busiYear).getLeft()
                    .plusDays(weekOffsetOfNextQuarter * 7).minusDays(1);
        }
    }

    private int getBusiYearFromQuarterPeriod(int period) {
        if (period >= 0) {
            return startYear + period / 4;
        } else {
            return startYear + (period + 1) / 4 - 1;
        }
    }

    private int getQuarterOffsetInBusiYearFromQuarterPeriod(int period) {
        if (period >= 0) {
            return period % 4;
        } else {
            return (period % 4 + 4) % 4;
        }
    }
}
