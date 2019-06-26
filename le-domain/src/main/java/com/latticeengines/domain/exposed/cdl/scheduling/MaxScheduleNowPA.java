package com.latticeengines.domain.exposed.cdl.scheduling;

public class MaxScheduleNowPA implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target) {
        return currentState.getCanRunScheduleNowJobCount() < 1 && target.isScheduledNow();
    }

    @Override
    public String getName() {
        return MaxScheduleNowPA.class.getName();
    }
}
