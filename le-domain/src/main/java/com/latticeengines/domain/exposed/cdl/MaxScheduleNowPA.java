package com.latticeengines.domain.exposed.cdl;

public class MaxScheduleNowPA implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target) {
        return currentState.getCanRunScheduleNowJobCount() >= 1 || !target.isScheduledNow();
    }
}
