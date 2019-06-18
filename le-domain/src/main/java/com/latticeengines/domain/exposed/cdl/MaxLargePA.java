package com.latticeengines.domain.exposed.cdl;

import java.util.Set;

public class MaxLargePA implements Constraint {

    @Override
    public boolean checkViolated(SystemStatus currentState, Set<String> scheduledTenants, TenantActivity target) {
        return currentState.getCanRunLargeJobCount() < 1 && target.isLarge();
    }
}
