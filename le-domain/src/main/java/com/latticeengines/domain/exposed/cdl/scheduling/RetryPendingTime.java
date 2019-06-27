package com.latticeengines.domain.exposed.cdl.scheduling;

public class RetryPendingTime implements Constraint {

    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target) {
        return target.getLastFinishTime() - (schedulingPATimeClock.getCurrentTime() - 6*7*24*3600000L) < 0;
    }

    @Override
    public String getName() {
        return RetryPendingTime.class.getName();
    }
}
