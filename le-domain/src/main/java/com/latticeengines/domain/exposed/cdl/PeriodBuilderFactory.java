package com.latticeengines.domain.exposed.cdl;

import com.latticeengines.common.exposed.period.CalendarMonthPeriodBuilder;
import com.latticeengines.common.exposed.period.CalendarQuarterPeriodBuilder;
import com.latticeengines.common.exposed.period.CalendarWeekPeriodBuilder;
import com.latticeengines.common.exposed.period.CalendarYearPeriodBuilder;
import com.latticeengines.common.exposed.period.PeriodBuilder;

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
                throw new UnsupportedOperationException("Unknown strategy template " + strategy.getTemplate());
        }
    }

    private static PeriodBuilder buildWeekPeriod(PeriodStrategy strategy) {
        return new CalendarWeekPeriodBuilder(strategy.getStartTimeStr());
    }

    private static PeriodBuilder buildMonthPeriod(PeriodStrategy strategy) {
        return new CalendarMonthPeriodBuilder(strategy.getStartTimeStr());
    }

    private static PeriodBuilder buildQuarterPeriod(PeriodStrategy strategy) {
        return new CalendarQuarterPeriodBuilder(strategy.getStartTimeStr());
    }

    private static PeriodBuilder buildYearPeriod(PeriodStrategy strategy) {
        return new CalendarYearPeriodBuilder(strategy.getStartTimeStr());
    }

}
