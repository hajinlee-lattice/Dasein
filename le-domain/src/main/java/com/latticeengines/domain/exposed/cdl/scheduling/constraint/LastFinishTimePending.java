package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public class LastFinishTimePending implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        if (target.getLastFinishTime() == null) {
            return true;
        }
        Long currentTime = timeClock.getCurrentTime();
        currentTime = (currentTime - target.getLastFinishTime()) / 60000;
        return target.getLastFinishTime() == 0L || currentTime < 15;
    }

    @Override
    public String getName() {
        return LastFinishTimePending.class.getName();
    }
}
