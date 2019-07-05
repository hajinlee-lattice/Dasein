package com.latticeengines.domain.exposed.cdl.scheduling;

public class RetryExist implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        return !target.isRetry();
    }

    @Override
    public String getName() {
        return RetryExist.class.getName();
    }
}
