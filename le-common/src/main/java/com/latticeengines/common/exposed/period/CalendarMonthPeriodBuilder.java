package com.latticeengines.common.exposed.period;

import java.time.LocalDate;
import java.time.temporal.ChronoField;

public class CalendarMonthPeriodBuilder implements PeriodBuilder {

    private static final long serialVersionUID = -8736107964245177059L;

    private int getPeriodsBetweenDates(LocalDate start, LocalDate end) {
        int months = getMonth(end) - getMonth(start);
        int years = end.get(ChronoField.YEAR) - start.get(ChronoField.YEAR);
        months += years * 12;
        return Math.abs(months);
    }

    @Override
    public int toPeriodId(String date) {
        return getPeriodsBetweenDates(T0, LocalDate.parse(date));
    }

    @Override
    public int toPeriodId(LocalDate date) {
        return getPeriodsBetweenDates(T0, date);
    }

    private static int getMonth(LocalDate cal) {
        return cal.get(ChronoField.MONTH_OF_YEAR);
    }

}
