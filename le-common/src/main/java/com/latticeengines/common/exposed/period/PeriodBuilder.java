package com.latticeengines.common.exposed.period;

import java.io.Serializable;
import java.time.LocalDate;

import org.apache.commons.lang3.tuple.Pair;

public interface PeriodBuilder extends Serializable {

    int toPeriodId(String date);

    Pair<LocalDate, LocalDate> toDateRange(int startPeriod, int endPeriod);

}
