package com.latticeengines.domain.exposed.period;

import java.time.LocalDate;

import org.apache.commons.lang3.tuple.Pair;

public abstract class BasePeriodBuilder implements PeriodBuilder {

    private static final long serialVersionUID = -7573365240345035161L;

    protected abstract LocalDate getStartDate(int period);
    protected abstract LocalDate getEndDate(int period);

    @Override
    public Pair<LocalDate, LocalDate> toDateRange(int startPeriod, int endPeriod) {
        LocalDate startDate = getStartDate(startPeriod);
        LocalDate endDate = getEndDate(endPeriod);
        return Pair.of(startDate, endDate);
    }

}
