package com.latticeengines.domain.exposed.cdl.scheduling;

public class LastFinishTimePending implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target) {
        if (target.getLastFinishTime() == null) {
            return true;
        }
        Long currentTime = schedulingPATimeClock.getCurrentTime();
        currentTime = (currentTime - target.getLastFinishTime()) / 60000;
        return target.getLastFinishTime() == 0L || currentTime < 15;
    }

    @Override
    public String getName() {
        return LastFinishTimePending.class.getName();
    }
}
