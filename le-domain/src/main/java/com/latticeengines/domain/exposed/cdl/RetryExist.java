package com.latticeengines.domain.exposed.cdl;

import java.util.Set;

public class RetryExist implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, Set<String> scheduledTenants, TenantActivity target) {
        return !target.isRetry();
    }
}
