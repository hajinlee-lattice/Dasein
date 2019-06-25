package com.latticeengines.domain.exposed.cdl;

public class RetryExist implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target) {
        return !target.isRetry();
    }

    @Override
    public String getName() {
        return RetryExist.class.getName();
    }
}
