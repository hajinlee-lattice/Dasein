package com.latticeengines.domain.exposed.period;

import java.time.LocalDate;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

public class BusinessYearPeriodBuilder extends BusinessCalendarBasedPeriodBuilder
        implements PeriodBuilder {

    private static final long serialVersionUID = 2840113949282037628L;

    public BusinessYearPeriodBuilder(BusinessCalendar businessCalendar) {
        super(businessCalendar);
    }

    public BusinessYearPeriodBuilder(BusinessCalendar businessCalendar, int startYear) {
        super(businessCalendar, startYear);
    }

    /*
     * PeriodId starts with 0
     */
    @Override
    public int toPeriodId(String date) {
        LocalDate evalDate = LocalDate.parse(date);
        Pair<LocalDate, LocalDate> busiYearDateRange = getDateRangeOfYear(evalDate.getYear());
        int businessYear = getBusinessYear(evalDate, busiYearDateRange);
        return businessYear - startYear;
    }

    @Override
    protected LocalDate getStartDate(int period) {
        int busiYear = startYear + period;
        return getDateRangeOfYear(busiYear).getLeft();
    }

    @Override
    protected LocalDate getEndDate(int period) {
        int nextBusiYear = startYear + period + 1;
        // First day of next business year - 1
        return getDateRangeOfYear(nextBusiYear).getLeft().minusDays(1);
    }

}
