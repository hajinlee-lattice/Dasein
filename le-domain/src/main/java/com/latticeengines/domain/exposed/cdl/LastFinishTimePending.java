package com.latticeengines.domain.exposed.cdl;

import java.util.Date;
import java.util.Set;

public class LastFinishTimePending implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, Set<String> scheduledTenants, TenantActivity target) {
        if (target.getLastFinishTime() == null) {
            return true;
        }
        Long currentTime = new Date().getTime();
        currentTime = (currentTime - target.getLastFinishTime()) / 60000;
        return target.getLastFinishTime() == 0L || currentTime < 15;
    }
}
