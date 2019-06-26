package com.latticeengines.domain.exposed.cdl.scheduling;

public class LastActionTimePending implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target) {
        if (target.getLastActionTime() == null || target.getLastActionTime() == 0L) {
            return true;
        }
        long currentTime = schedulingPATimeClock.getCurrentTime();
        long lastMinute = (currentTime - target.getLastActionTime()) / 60000;
        return lastMinute < 10;
    }

    @Override
    public String getName() {
        return LastActionTimePending.class.getName();
    }
}
