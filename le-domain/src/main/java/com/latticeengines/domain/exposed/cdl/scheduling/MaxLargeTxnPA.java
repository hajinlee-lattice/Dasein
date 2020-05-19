package com.latticeengines.domain.exposed.cdl.scheduling;

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
