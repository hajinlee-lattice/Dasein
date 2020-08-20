package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public class MaxPA implements Constraint {
    @Override
    public ConstraintValidationResult validate(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        return new ConstraintValidationResult(currentState.getCanRunJobCount() < 1,
                "too many PAs are running right now");
    }

    @Override
    public String getName() {
        return MaxPA.class.getName();
    }
}
