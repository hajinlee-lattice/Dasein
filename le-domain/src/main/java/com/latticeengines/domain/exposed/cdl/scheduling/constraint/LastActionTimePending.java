package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public class LastActionTimePending implements Constraint {
    @Override
    public ConstraintValidationResult validate(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        if (target.getLastActionTime() == null || target.getLastActionTime() == 0L) {
            return ConstraintValidationResult.VALID;
        }
        long currentTime = timeClock.getCurrentTime();
        long minSinceLastAction = (currentTime - target.getLastActionTime()) / 60000;
        return new ConstraintValidationResult(minSinceLastAction < 10, "there are recent activities in this tenant");
    }

    @Override
    public String getName() {
        return LastActionTimePending.class.getName();
    }
}
