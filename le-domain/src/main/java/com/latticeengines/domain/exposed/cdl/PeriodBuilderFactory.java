package com.latticeengines.domain.exposed.cdl;

import com.latticeengines.common.exposed.period.NaturalMonthPeriodBuilder;
import com.latticeengines.common.exposed.period.NaturalQuarterPeriodBuilder;
import com.latticeengines.common.exposed.period.NaturalWeekPeriodBuilder;
import com.latticeengines.common.exposed.period.NaturalYearPeriodBuilder;
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
        return new NaturalWeekPeriodBuilder(strategy.getStartTimeStr());
    }

    private static PeriodBuilder buildMonthPeriod(PeriodStrategy strategy) {
        return new NaturalMonthPeriodBuilder(strategy.getStartTimeStr());
    }

    private static PeriodBuilder buildQuarterPeriod(PeriodStrategy strategy) {
        return new NaturalQuarterPeriodBuilder(strategy.getStartTimeStr());
    }

    private static PeriodBuilder buildYearPeriod(PeriodStrategy strategy) {
        return new NaturalYearPeriodBuilder(strategy.getStartTimeStr());
    }

}
