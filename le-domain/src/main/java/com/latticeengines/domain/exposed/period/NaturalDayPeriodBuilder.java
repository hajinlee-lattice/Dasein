package com.latticeengines.domain.exposed.period;

import static java.time.temporal.ChronoUnit.DAYS;

import java.time.LocalDate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NaturalDayPeriodBuilder extends StartTimeBasedPeriodBuilder implements PeriodBuilder {

    private static final Logger log = LoggerFactory.getLogger(NaturalDayPeriodBuilder.class);

    NaturalDayPeriodBuilder() {
        super();
    }

    @Override
    protected LocalDate getStartDate(int period) {
        return startDate.plusDays(period);
    }

    @Override
    protected LocalDate getEndDate(int period) {
        return startDate.plusDays(period);
    }

    @Override
    protected int getPeriodsBetweenDates(LocalDate start, LocalDate end) {
        long days = DAYS.between(start, end);
        return Long.valueOf(days).intValue();
    }

}
