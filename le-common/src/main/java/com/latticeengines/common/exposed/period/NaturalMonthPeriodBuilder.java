package com.latticeengines.common.exposed.period;

import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAdjusters;

public class NaturalMonthPeriodBuilder extends StartTimeBasedPeriodBuilder implements PeriodBuilder {

    private static final long serialVersionUID = -8736107964245177059L;

    public NaturalMonthPeriodBuilder() {
        super();
    }

    public NaturalMonthPeriodBuilder(String startDate) {
        super(startDate);
    }

    @Override
    protected LocalDate getStartDate(int period) {
        int year = getPeriodYear(period);
        int month = getPeriodMonth(period);
        return LocalDate.of(year, month, 1);
    }

    @Override
    protected LocalDate getEndDate(int period) {
        LocalDate firstDayOfMonth = getStartDate(period);
        return firstDayOfMonth.with(TemporalAdjusters.lastDayOfMonth());
    }

    @Override
    protected int getPeriodsBetweenDates(LocalDate start, LocalDate end) {
        int months = getMonth(end) - getMonth(start);
        int years = end.get(ChronoField.YEAR) - start.get(ChronoField.YEAR);
        months += years * 12;
        return months;
    }

    private static int getMonth(LocalDate cal) {
        return cal.get(ChronoField.MONTH_OF_YEAR);
    }

    private int getPeriodYear(int period) {
        int month = startDate.get(ChronoField.MONTH_OF_YEAR) + period;
        if (month > 0) {
            return startDate.get(ChronoField.YEAR) + ((month - 1) / 12);
        } else  {
            return startDate.get(ChronoField.YEAR) + (month / 12) - 1;
        }
    }

    private int getPeriodMonth(int period) {
        int month = startDate.get(ChronoField.MONTH_OF_YEAR) + period;
        if (month > 0) {
            return (month - 1) % 12 + 1;
        } else  {
            return month % 12 + 12;
        }
    }

}
