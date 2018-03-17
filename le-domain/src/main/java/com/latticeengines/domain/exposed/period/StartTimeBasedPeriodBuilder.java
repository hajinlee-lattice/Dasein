package com.latticeengines.domain.exposed.period;

import java.time.LocalDate;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;

public abstract class StartTimeBasedPeriodBuilder extends BasePeriodBuilder implements PeriodBuilder {

    private static final long serialVersionUID = -5148098981943162677L;

    private static final LocalDate DEFAULT_FIRST_DATE = LocalDate.of(2000, 1, 1);

    private static LocalDate OVERWRITING_NOW;

    protected LocalDate startDate;

    protected StartTimeBasedPeriodBuilder() {
        this(DEFAULT_FIRST_DATE);
    }

    public StartTimeBasedPeriodBuilder(String startDate) {
        this(StringUtils.isBlank(startDate) ? DEFAULT_FIRST_DATE : LocalDate.parse(startDate));
    }

    public StartTimeBasedPeriodBuilder(LocalDate startDate) {
        this.startDate = startDate;
    }

    @Override
    public int toPeriodId(String date) {
        return toPeriodId(LocalDate.parse(date));
    }

    // Not sure when this should be used
    private int currentPeriod() {
        if (OVERWRITING_NOW != null) {
            return toPeriodId(OVERWRITING_NOW);
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
        OVERWRITING_NOW = fakedNow;
    }

}
