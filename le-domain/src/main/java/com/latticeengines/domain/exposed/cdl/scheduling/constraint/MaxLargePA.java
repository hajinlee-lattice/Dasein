package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public class MaxLargePA implements Constraint {

    @Override
    public ConstraintValidationResult validate(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        return new ConstraintValidationResult(currentState.getCanRunLargeJobCount() < 1 && target.isLarge(),
                "too many large PAs are running right now");
    }

    @Override
    public String getName() {
        return MaxLargePA.class.getName();
    }
}
