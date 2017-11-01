package com.latticeengines.common.exposed.period;

import java.io.Serializable;
import java.time.LocalDate;

public interface PeriodBuilder extends Serializable {

    LocalDate T0 = LocalDate.parse("1987-12-20");

    // int getPeriodsBetweenDates(LocalDate start, LocalDate end);

    // int getPeriodsBetweenDates(String start, String end);

    int toPeriodId(String date);

    int toPeriodId(LocalDate date);

}
