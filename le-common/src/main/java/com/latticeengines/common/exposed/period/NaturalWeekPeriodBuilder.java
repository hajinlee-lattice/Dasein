package com.latticeengines.common.exposed.period;

import static java.time.temporal.ChronoUnit.DAYS;

import java.time.LocalDate;
import java.time.temporal.ChronoField;

public class NaturalWeekPeriodBuilder extends StartTimeBasedPeriodBuilder implements PeriodBuilder {

    private static final long serialVersionUID = 9077761645002879654L;

    private int startDayOfWeek;

    public NaturalWeekPeriodBuilder() {
        super();
        setStartDayOfWeek();
    }

    public NaturalWeekPeriodBuilder(String startDate) {
        super(startDate);
        setStartDayOfWeek();
    }

    @Override
    protected LocalDate getStartDate(int period) {
        int days = 7 * period - startDayOfWeek;
        return startDate.plusDays(days);
    }

    @Override
    protected LocalDate getEndDate(int period) {
        int days = 7 * period + (6 - startDayOfWeek);
        return startDate.plusDays(days);
    }

    @Override
    protected int getPeriodsBetweenDates(LocalDate start, LocalDate end) {
        long days = DAYS.between(start, end);
        days += startDayOfWeek;
        if (days >= 0) {
            return (int) (days / 7);
        } else {
            return (int) ((days + 1) / 7) - 1;
        }
    }

    private void setStartDayOfWeek() {
        // Sunday (0) to Saturday (6)
        startDayOfWeek = startDate.get(ChronoField.DAY_OF_WEEK) % 7;
    }

    private int getPeriodYear(int period) {
        return startDate.get(ChronoField.YEAR) + period;
    }

}
