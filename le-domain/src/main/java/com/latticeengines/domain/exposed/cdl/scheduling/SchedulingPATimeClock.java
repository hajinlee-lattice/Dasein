package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.Date;

public class SchedulingPATimeClock implements TimeClock {

    @Override
    public long getCurrentTime() {
        return new Date().getTime();
    }
}
