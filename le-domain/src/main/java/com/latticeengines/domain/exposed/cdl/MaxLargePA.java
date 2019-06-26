package com.latticeengines.domain.exposed.cdl;

public class MaxLargePA implements Constraint {

    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target) {
        return currentState.getCanRunLargeJobCount() < 1 && target.isLarge();
    }

    @Override
    public String getName() {
        return MaxLargePA.class.getName();
    }
}
