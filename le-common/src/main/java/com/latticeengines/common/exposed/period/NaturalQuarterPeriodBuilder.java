package com.latticeengines.common.exposed.period;

import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAdjusters;

public class NaturalQuarterPeriodBuilder extends StartTimeBasedPeriodBuilder implements PeriodBuilder {

    private static final long serialVersionUID = -4975605566623744278L;

    NaturalQuarterPeriodBuilder() {
        super();
    }

    public NaturalQuarterPeriodBuilder(String startDate) {
        super(startDate);
    }

    @Override
    protected LocalDate getStartDate(int period) {
        int year = getPeriodYear(period);
        int quarter = getPeriodQuarter(period);
        int month = quarter * 3 + 1;
        return LocalDate.of(year, month, 1);
    }

    @Override
    protected LocalDate getEndDate(int period) {
        int year = getPeriodYear(period);
        int quarter = getPeriodQuarter(period);
        int month = quarter * 3 + 3;
        LocalDate date = LocalDate.of(year, month, 1);
        return date.with(TemporalAdjusters.lastDayOfMonth());
    }

    @Override
    protected int getPeriodsBetweenDates(LocalDate start, LocalDate end) {
        int quarters = getQuarter(end) - getQuarter(start);
        int years = end.get(ChronoField.YEAR) - start.get(ChronoField.YEAR);
        quarters += years * 4;
        return quarters;
    }

    private static int getQuarter(LocalDate cal) {
        return (cal.get(ChronoField.MONTH_OF_YEAR) - 1) / 3;
    }

    private int getPeriodYear(int period) {
        int quarter = getQuarter(startDate) + period;
        if (quarter >= 0) {
            return startDate.get(ChronoField.YEAR) + (quarter / 4);
        } else  {
            return startDate.get(ChronoField.YEAR) + ((quarter + 1) / 4) - 1;
        }
    }

    private int getPeriodQuarter(int period) {
        int quarter = getQuarter(startDate) + period;
        if (quarter >= 0) {
            return quarter % 4;
        } else  {
            return (quarter + 1) % 4 + 3;
        }
    }



}
