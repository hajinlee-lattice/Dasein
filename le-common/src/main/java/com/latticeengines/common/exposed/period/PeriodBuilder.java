package com.latticeengines.common.exposed.period;

import java.io.Serializable;
import java.time.LocalDate;

public interface PeriodBuilder extends Serializable {

    int toPeriodId(String date);

}
