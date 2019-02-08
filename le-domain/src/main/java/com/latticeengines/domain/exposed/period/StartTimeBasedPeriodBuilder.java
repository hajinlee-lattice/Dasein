package com.latticeengines.domain.exposed.period;

import java.time.LocalDate;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

public abstract class StartTimeBasedPeriodBuilder extends BasePeriodBuilder
        implements PeriodBuilder {

    private static final long serialVersionUID = -5148098981943162677L;

    private static final LocalDate DEFAULT_FIRST_DATE = LocalDate.of(2000, 1, 1);

    protected LocalDate startDate;

    StartTimeBasedPeriodBuilder() {
        this(DEFAULT_FIRST_DATE);
    }

    StartTimeBasedPeriodBuilder(String startDate) {
        this(StringUtils.isBlank(startDate) ? DEFAULT_FIRST_DATE : LocalDate.parse(startDate));
    }

    private StartTimeBasedPeriodBuilder(LocalDate startDate) {
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

    @Override
    public Pair<LocalDate, LocalDate> getDateRangeOfYear(int year) {
        return Pair.of(LocalDate.of(year, 1, 1), LocalDate.of(year, 12, 31));
    }

}
