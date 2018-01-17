package com.latticeengines.common.exposed.period;

import java.time.LocalDate;
import java.time.temporal.ChronoField;

public class CalendarYearPeriodBuilder extends StartTimeBasedPeriodBuilder implements PeriodBuilder {

    private static final long serialVersionUID = -3123543233939069357L;

    public CalendarYearPeriodBuilder() {
        super();
    }

    public CalendarYearPeriodBuilder(String startDate) {
        super(startDate);
    }

    @Override
    protected int getPeriodsBetweenDates(LocalDate start, LocalDate end) {
        int years = end.get(ChronoField.YEAR) - start.get(ChronoField.YEAR);
        return years + 1;
    }

}
