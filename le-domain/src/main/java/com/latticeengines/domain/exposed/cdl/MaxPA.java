package com.latticeengines.domain.exposed.cdl;

public class MaxPA implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target) {
        return currentState.getCanRunJobCount() >= 1;
    }
}
