package com.latticeengines.common.exposed.period;

import java.time.LocalDate;
import java.time.temporal.ChronoField;

public class CalendarMonthPeriodBuilder extends StartTimeBasedPeriodBuilder implements PeriodBuilder {

    private static final long serialVersionUID = -8736107964245177059L;

    public CalendarMonthPeriodBuilder(String startDate) {
        super(startDate);
    }

    @Override
    protected int getPeriodsBetweenDates(LocalDate start, LocalDate end) {
        int months = getMonth(end) - getMonth(start);
        int years = end.get(ChronoField.YEAR) - start.get(ChronoField.YEAR);
        months += years * 12;
        return months + 1;
    }

    private static int getMonth(LocalDate cal) {
        return cal.get(ChronoField.MONTH_OF_YEAR);
    }

}
