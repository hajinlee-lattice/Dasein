package com.latticeengines.common.exposed.period;

import java.time.LocalDate;
import java.time.temporal.ChronoField;

@SuppressWarnings("serial")
public class CalendarMonthPeriodBuilder implements PeriodBuilder {

    @Override
    public int getPeriodsBetweenDates(String startStr, String endStr) {
        LocalDate start = LocalDate.parse(startStr);
        LocalDate end = LocalDate.parse(endStr);
        return getPeriodsBetweenDates(start, end);
    }

    public int getPeriodsBetweenDates(LocalDate start, LocalDate end) {
        int months = getMonth(end) - getMonth(start);
        int years = end.get(ChronoField.YEAR) - start.get(ChronoField.YEAR);
        months += years * 12;
        return Math.abs(months);
    }

    public static int getMonth(LocalDate cal) {
        int month = cal.get(ChronoField.MONTH_OF_YEAR);
        return month;
    }

}
