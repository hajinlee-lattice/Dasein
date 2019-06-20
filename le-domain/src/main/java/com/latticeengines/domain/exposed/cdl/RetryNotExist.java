package com.latticeengines.domain.exposed.cdl;

import java.util.Set;

public class RetryNotExist implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, Set<String> scheduledTenants, TenantActivity target) {
        return target.isRetry();
    }

    @Override
    public String getName() {
        return RetryNotExist.class.getName();
    }
}
