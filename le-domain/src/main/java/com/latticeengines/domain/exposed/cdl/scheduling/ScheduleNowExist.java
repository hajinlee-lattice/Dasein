package com.latticeengines.domain.exposed.cdl.scheduling;

public class ScheduleNowExist implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        return !target.isScheduledNow();
    }

    @Override
    public String getName() {
        return ScheduleNowExist.class.getName();
    }
}
