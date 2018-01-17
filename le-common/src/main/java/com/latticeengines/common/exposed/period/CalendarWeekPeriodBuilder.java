package com.latticeengines.common.exposed.period;

import static java.time.temporal.ChronoUnit.DAYS;

import java.time.LocalDate;
import java.time.temporal.ChronoField;

public class CalendarWeekPeriodBuilder extends StartTimeBasedPeriodBuilder implements PeriodBuilder {

    private static final long serialVersionUID = 9077761645002879654L;

    private static int startDayOfWeek;

    public CalendarWeekPeriodBuilder() {
        super();
        setStartDayOfWeek();
    }

    public CalendarWeekPeriodBuilder(String startDate) {
        super(startDate);
        setStartDayOfWeek();
    }

    @Override
    protected int getPeriodsBetweenDates(LocalDate start, LocalDate end) {
        long days = DAYS.between(start, end);
        days += startDayOfWeek - 1;
        if (days >= 0) {
            return (int) (days / 7) + 1;
        } else {
            return - (int) (-days / 7);
        }
    }

    private void setStartDayOfWeek() {
        // Monday (1) to Sunday (7)
        startDayOfWeek = startDate.get(ChronoField.DAY_OF_WEEK);
    }

}
