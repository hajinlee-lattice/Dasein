package com.latticeengines.common.exposed.period;

import java.time.LocalDate;

import com.google.common.annotations.VisibleForTesting;

public abstract class StartTimeBasedPeriodBuilder implements PeriodBuilder {

    private static final long serialVersionUID = -5148098981943162677L;

    private static final LocalDate DEFAULT_FIRST_DATE = LocalDate.of(2000, 1, 1);

    private static LocalDate OVERWRITTING_NOW;

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

    @Override
    public int currentPeriod() {
        if (OVERWRITTING_NOW != null) {
            return toPeriodId(OVERWRITTING_NOW);
        } else {
            return toPeriodId(LocalDate.now());
        }
    }

    private int toPeriodId(LocalDate date) {
        return getPeriodsBetweenDates(startDate, date);
    }

    protected abstract int getPeriodsBetweenDates(LocalDate start, LocalDate end);

    @VisibleForTesting
    public void overwriteNow(LocalDate fakedNow) {
        OVERWRITTING_NOW = fakedNow;
    }

}
