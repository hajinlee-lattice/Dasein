package com.latticeengines.domain.exposed.cdl.scheduling;

public class LastActionTimePending implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        if (target.getLastActionTime() == null || target.getLastActionTime() == 0L) {
            return true;
        }
        long currentTime = timeClock.getCurrentTime();
        long lastMinute = (currentTime - target.getLastActionTime()) / 60000;
        return lastMinute < 10;
    }

    @Override
    public String getName() {
        return LastActionTimePending.class.getName();
    }
}
