package com.latticeengines.common.exposed.period;

import java.time.LocalDate;

public abstract class StartTimeBasedPeriodBuilder implements PeriodBuilder {

    private static final LocalDate DEFAULT_FIRST_DATE = LocalDate.of(2000, 1, 1);

    protected LocalDate startDate;

    protected StartTimeBasedPeriodBuilder() {
        this(DEFAULT_FIRST_DATE);
    }

    public StartTimeBasedPeriodBuilder(String startDate) {
        this(LocalDate.parse(startDate));
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
