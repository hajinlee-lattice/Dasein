package com.latticeengines.domain.exposed.cdl;

public class ScheduleNowExist implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target) {
        return !target.isScheduledNow();
    }

    @Override
    public String getName() {
        return ScheduleNowExist.class.getName();
    }
}
