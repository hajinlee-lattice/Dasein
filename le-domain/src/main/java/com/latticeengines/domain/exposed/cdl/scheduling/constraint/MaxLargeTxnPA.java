package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public class MaxLargeTxnPA implements Constraint {

    @Override
    public ConstraintValidationResult validate(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        return new ConstraintValidationResult(
                currentState.getCanRunLargeTxnJobCount() < 1 && target.isLargeTransaction(),
                "too many large transaction PAs are running right now");
    }

    @Override
    public String getName() {
        return MaxLargeTxnPA.class.getName();
    }
}
