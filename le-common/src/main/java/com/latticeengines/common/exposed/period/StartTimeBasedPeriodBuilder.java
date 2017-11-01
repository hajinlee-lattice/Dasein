package com.latticeengines.common.exposed.period;

import java.time.LocalDate;

public abstract class StartTimeBasedPeriodBuilder implements PeriodBuilder {

    protected LocalDate startDate;

    public StartTimeBasedPeriodBuilder(String startDate) {
        this.startDate = LocalDate.parse(startDate);
    }

    public StartTimeBasedPeriodBuilder(LocalDate startDate) {
        this.startDate = startDate;
    }

    @Override
    public int toPeriodId(String date) {
        return toPeriodId(LocalDate.parse(date));
    }

    private int toPeriodId(LocalDate date) {
        return getPeriodsBetweenDates(startDate, date);
    }

    protected abstract int getPeriodsBetweenDates(LocalDate start, LocalDate end);

}
