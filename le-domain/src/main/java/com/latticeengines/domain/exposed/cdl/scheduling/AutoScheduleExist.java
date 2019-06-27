package com.latticeengines.domain.exposed.cdl.scheduling;

public class AutoScheduleExist implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        return !target.isAutoSchedule();
    }

    @Override
    public String getName() {
        return AutoScheduleExist.class.getName();
    }
}
