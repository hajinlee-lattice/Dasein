package com.latticeengines.domain.exposed.period;

import java.time.LocalDate;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

public class BusinessMonthPeriodBuilder extends BusinessCalendarBasedPeriodBuilder
        implements PeriodBuilder {

    private static final long serialVersionUID = 6879465077306719958L;

    public BusinessMonthPeriodBuilder(BusinessCalendar businessCalendar) {
        super(businessCalendar);
    }

    public BusinessMonthPeriodBuilder(BusinessCalendar businessCalendar, int startYear) {
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
        int busiYear = getBusinessYear(evalDate, busiYearDateRange);
        if (busiYear != evalDate.getYear()) {
            busiYearDateRange = getDateRangeOfYear(busiYear);
        }

        int weekOffsetInBusiYear = getWeekOffsetInBusiYear(evalDate, busiYearDateRange);
        return 12 * (busiYear - startYear) + getMonthOffsetInBusiYear(weekOffsetInBusiYear);
    }

    @Override
    protected LocalDate getStartDate(int period) {
        int busiYear = getBusiYearFromMonthPeriod(period);
        int monthOffsetInBusiYear = getMonthOffsetInBusiYearFromMonthPeriod(period);
        int weekOffsetInBusiYear = getWeekOffsetInBusiYear(monthOffsetInBusiYear);
        return getDateRangeOfYear(busiYear).getLeft().plusDays(weekOffsetInBusiYear * 7);
    }

    @Override
    protected LocalDate getEndDate(int period) {
        int busiYear = getBusiYearFromMonthPeriod(period);
        int monthOffsetInBusiYear = getMonthOffsetInBusiYearFromMonthPeriod(period);

        if (monthOffsetInBusiYear == 11) {
            // First day of next business year - 1
            return getDateRangeOfYear(busiYear + 1).getLeft().minusDays(1);
        } else {
            int weekOffsetOfNextMonth = getWeekOffsetInBusiYear(monthOffsetInBusiYear + 1);
            // First day of next business month - 1
            return getDateRangeOfYear(busiYear).getLeft()
                    .plusDays(weekOffsetOfNextMonth * 7).minusDays(1);
        }
    }

    private int getBusiYearFromMonthPeriod(int period) {
        if (period >= 0) {
            return startYear + period / 12;
        } else {
            return startYear + (period + 1) / 12 - 1;
        }
    }

    private int getMonthOffsetInBusiYearFromMonthPeriod(int period) {
        if (period >= 0) {
            return period % 12;
        } else {
            return (period % 12 + 12) % 12;
        }
    }

}
