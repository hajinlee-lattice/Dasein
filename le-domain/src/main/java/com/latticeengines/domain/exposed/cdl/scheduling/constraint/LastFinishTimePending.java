package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public class LastFinishTimePending implements Constraint {
    @Override
    public ConstraintValidationResult validate(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        if (target.getLastFinishTime() == null) {
            return new ConstraintValidationResult(true, null);
        }
        long currentTime = timeClock.getCurrentTime();
        currentTime = (currentTime - target.getLastFinishTime()) / 60000;
        return new ConstraintValidationResult(target.getLastFinishTime() == 0L || currentTime < 15, null);
    }

    @Override
    public String getName() {
        return LastFinishTimePending.class.getName();
    }
}
