package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public class MaxScheduleNowPA implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        return currentState.getCanRunScheduleNowJobCount() < 1 && target.isScheduledNow();
    }

    @Override
    public String getName() {
        return MaxScheduleNowPA.class.getName();
    }
}
