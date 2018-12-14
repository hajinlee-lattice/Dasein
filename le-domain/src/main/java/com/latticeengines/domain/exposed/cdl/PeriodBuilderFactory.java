package com.latticeengines.domain.exposed.cdl;

import com.latticeengines.domain.exposed.period.BusinessMonthPeriodBuilder;
import com.latticeengines.domain.exposed.period.BusinessQuarterPeriodBuilder;
import com.latticeengines.domain.exposed.period.BusinessWeekPeriodBuilder;
import com.latticeengines.domain.exposed.period.BusinessYearPeriodBuilder;
import com.latticeengines.domain.exposed.period.NaturalMonthPeriodBuilder;
import com.latticeengines.domain.exposed.period.NaturalQuarterPeriodBuilder;
import com.latticeengines.domain.exposed.period.NaturalWeekPeriodBuilder;
import com.latticeengines.domain.exposed.period.NaturalYearPeriodBuilder;
import com.latticeengines.domain.exposed.period.PeriodBuilder;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

public final class PeriodBuilderFactory {

    public static PeriodBuilder build(PeriodStrategy strategy) {
        switch (strategy.getTemplate()) {
            case Week:
                return buildWeekPeriod(strategy);
            case Month:
                return buildMonthPeriod(strategy);
            case Quarter:
                return buildQuarterPeriod(strategy);
            case Year:
                return buildYearPeriod(strategy);
            default:
                throw new UnsupportedOperationException(
                        "Unknown strategy template " + strategy.getTemplate());
        }
    }

    private static PeriodBuilder buildWeekPeriod(PeriodStrategy strategy) {
        BusinessCalendar businessCalendar = strategy.getBusinessCalendar();
        if (!isNaturalCalendar(businessCalendar)) {
            return new BusinessWeekPeriodBuilder(businessCalendar);
        } else {
            return new NaturalWeekPeriodBuilder(strategy.getStartTimeStr());
        }
    }

    private static PeriodBuilder buildMonthPeriod(PeriodStrategy strategy) {
        BusinessCalendar businessCalendar = strategy.getBusinessCalendar();
        if (!isNaturalCalendar(businessCalendar)) {
            return new BusinessMonthPeriodBuilder(businessCalendar);
        } else {
            return new NaturalMonthPeriodBuilder(strategy.getStartTimeStr());
        }
    }

    private static PeriodBuilder buildQuarterPeriod(PeriodStrategy strategy) {
        BusinessCalendar businessCalendar = strategy.getBusinessCalendar();
        if (!isNaturalCalendar(businessCalendar)) {
            return new BusinessQuarterPeriodBuilder(businessCalendar);
        } else {
            return new NaturalQuarterPeriodBuilder(strategy.getStartTimeStr());
        }
    }

    private static PeriodBuilder buildYearPeriod(PeriodStrategy strategy) {
        BusinessCalendar businessCalendar = strategy.getBusinessCalendar();
        if (!isNaturalCalendar(businessCalendar)) {
            return new BusinessYearPeriodBuilder(businessCalendar);
        } else {
            return new NaturalYearPeriodBuilder(strategy.getStartTimeStr());
        }
    }

    private static boolean isNaturalCalendar(BusinessCalendar calendar) {
        return calendar == null || calendar.getMode() == BusinessCalendar.Mode.STANDARD;
    }

}
