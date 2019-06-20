package com.latticeengines.domain.exposed.cdl;

import java.util.Set;

public class MaxPA implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, Set<String> scheduledTenants, TenantActivity target) {
        return currentState.getCanRunJobCount() < 1;
    }

    @Override
    public String getName() {
        return MaxPA.class.getName();
    }
}
