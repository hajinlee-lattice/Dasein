package com.latticeengines.common.exposed.period;

import java.io.Serializable;

public interface PeriodBuilder extends Serializable {

    int toPeriodId(String date);

}
