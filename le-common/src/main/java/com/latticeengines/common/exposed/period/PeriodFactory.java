package com.latticeengines.common.exposed.period;

import java.util.HashMap;
import java.util.Map;

public class PeriodFactory {
    private static Map<PeriodStrategy, PeriodBuilder> builderMap = new HashMap<>();
    static {
        builderMap.put(PeriodStrategy.CalendarQuarter, new CalendarQuarterPeriodBuilder());
        builderMap.put(PeriodStrategy.CalendarMonth, new CalendarMonthPeriodBuilder());
    }

    public static PeriodBuilder getInstance(PeriodStrategy periodStrategy) {
        return builderMap.get(periodStrategy);
    }
}
