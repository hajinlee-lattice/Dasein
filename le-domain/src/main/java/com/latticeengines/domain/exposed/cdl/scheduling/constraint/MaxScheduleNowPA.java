package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public class MaxScheduleNowPA implements Constraint {
    @Override
    public ConstraintValidationResult validate(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        return new ConstraintValidationResult(
                currentState.getCanRunScheduleNowJobCount() < 1 && target.isScheduledNow(),
                "too many schedule now PAs are running right now");
    }

    @Override
    public String getName() {
        return MaxScheduleNowPA.class.getName();
    }
}
