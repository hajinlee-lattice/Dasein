package com.latticeengines.common.exposed.period;

import java.time.LocalDate;
import java.time.Month;
import java.time.temporal.ChronoField;

public class CalendarQuarterPeriodBuilder implements PeriodBuilder {

    private static final long serialVersionUID = -4975605566623744278L;

    @Override
    public int toPeriodId(String date) {
        return getPeriodsBetweenDates(T0, LocalDate.parse(date));
    }

    @Override
    public int toPeriodId(LocalDate date) {
        return getPeriodsBetweenDates(T0, date);
    }

    private int getPeriodsBetweenDates(LocalDate start, LocalDate end) {
        int quarters = getQuarter(end) - getQuarter(start);
        int years = end.get(ChronoField.YEAR) - start.get(ChronoField.YEAR);
        quarters += years * 4;
        return Math.abs(quarters);
    }

    private static int getQuarter(LocalDate cal) {
        int month = cal.get(ChronoField.MONTH_OF_YEAR);
        switch (Month.of(month)) {
        case JANUARY:
        case FEBRUARY:
        case MARCH:
        default:
            return 0;
        case APRIL:
        case MAY:
        case JUNE:
            return 1;
        case JULY:
        case AUGUST:
        case SEPTEMBER:
            return 2;
        case OCTOBER:
        case NOVEMBER:
        case DECEMBER:
            return 3;
        }
    }

}
