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
        if (strategy.getBusinessCalendar() != null) {
            return new BusinessWeekPeriodBuilder(strategy.getBusinessCalendar());
        } else {
            return new NaturalWeekPeriodBuilder(strategy.getStartTimeStr());
        }
    }

    private static PeriodBuilder buildMonthPeriod(PeriodStrategy strategy) {
        if (strategy.getBusinessCalendar() != null) {
            return new BusinessMonthPeriodBuilder(strategy.getBusinessCalendar());
        } else {
            return new NaturalMonthPeriodBuilder(strategy.getStartTimeStr());
        }
    }

    private static PeriodBuilder buildQuarterPeriod(PeriodStrategy strategy) {
        if (strategy.getBusinessCalendar() != null) {
            return new BusinessQuarterPeriodBuilder(strategy.getBusinessCalendar());
        } else {
            return new NaturalQuarterPeriodBuilder(strategy.getStartTimeStr());
        }
    }

    private static PeriodBuilder buildYearPeriod(PeriodStrategy strategy) {
        if (strategy.getBusinessCalendar() != null) {
            return new BusinessYearPeriodBuilder(strategy.getBusinessCalendar());
        } else {
            return new NaturalYearPeriodBuilder(strategy.getStartTimeStr());
        }
    }

}
