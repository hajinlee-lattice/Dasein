package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public class MaxLargeTxnPA implements Constraint {

    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        return currentState.getCanRunLargeTxnJobCount() < 1 && target.isLargeTransaction();
    }

    @Override
    public String getName() {
        return MaxLargeTxnPA.class.getName();
    }
}
