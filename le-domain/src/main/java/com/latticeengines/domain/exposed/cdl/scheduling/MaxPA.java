package com.latticeengines.domain.exposed.cdl.scheduling;

public class MaxPA implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target) {
        return currentState.getCanRunJobCount() < 1;
    }

    @Override
    public String getName() {
        return MaxPA.class.getName();
    }
}
