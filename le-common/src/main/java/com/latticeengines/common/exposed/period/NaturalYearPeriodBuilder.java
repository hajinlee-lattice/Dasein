package com.latticeengines.common.exposed.period;

import java.time.LocalDate;
import java.time.temporal.ChronoField;

public class NaturalYearPeriodBuilder extends StartTimeBasedPeriodBuilder implements PeriodBuilder {

    private static final long serialVersionUID = -3123543233939069357L;

    public NaturalYearPeriodBuilder() {
        super();
    }

    public NaturalYearPeriodBuilder(String startDate) {
        super(startDate);
    }

    @Override
    protected LocalDate getStartDate(int period) {
        int year = getPeriodYear(period);
        return LocalDate.of(year, 1, 1);
    }

    @Override
    protected LocalDate getEndDate(int period) {
        int year = getPeriodYear(period);
        return LocalDate.of(year, 12, 31);
    }

    @Override
    protected int getPeriodsBetweenDates(LocalDate start, LocalDate end) {
        return end.get(ChronoField.YEAR) - start.get(ChronoField.YEAR);
    }

    private int getPeriodYear(int period) {
        return startDate.get(ChronoField.YEAR) + period;
    }

}
