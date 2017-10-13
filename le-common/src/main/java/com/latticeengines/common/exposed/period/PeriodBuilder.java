package com.latticeengines.common.exposed.period;

import java.io.Serializable;
import java.time.LocalDate;

public interface PeriodBuilder extends Serializable {

    public int getPeriodsBetweenDates(LocalDate start, LocalDate end);

    public int getPeriodsBetweenDates(String start, String end);

}
