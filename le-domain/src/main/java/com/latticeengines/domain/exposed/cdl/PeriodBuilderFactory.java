package com.latticeengines.domain.exposed.cdl;

import com.latticeengines.common.exposed.period.CalendarMonthPeriodBuilder;
import com.latticeengines.common.exposed.period.CalendarQuarterPeriodBuilder;
import com.latticeengines.common.exposed.period.PeriodBuilder;

public final class PeriodBuilderFactory {

    public static PeriodBuilder build(PeriodStrategy strategy) {
        switch (strategy.getTemplate()) {
            case Month:
                return buildMonthPeriod(strategy);
            case Quarter:
                return buildQuarterPeriod(strategy);
            default:
                throw new UnsupportedOperationException("Unknown strategy template " + strategy.getTemplate());
        }
    }

    private static PeriodBuilder buildMonthPeriod(PeriodStrategy strategy) {
        return new CalendarMonthPeriodBuilder(strategy.getStartTimeStr());
    }

    private static PeriodBuilder buildQuarterPeriod(PeriodStrategy strategy) {
        return new CalendarQuarterPeriodBuilder(strategy.getStartTimeStr());
    }

}
