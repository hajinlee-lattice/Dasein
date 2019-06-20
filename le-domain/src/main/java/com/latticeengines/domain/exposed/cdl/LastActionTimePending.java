package com.latticeengines.domain.exposed.cdl;

import java.util.Date;
import java.util.Set;

public class LastActionTimePending implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, Set<String> scheduledTenants, TenantActivity target) {
        if (target.getLastActionTime() == null || target.getLastActionTime() == 0L) {
            return true;
        }
        long currentTime = new Date().getTime();
        long lastMinute = (currentTime - target.getLastActionTime()) / 60000;
        return lastMinute < 10;
    }

    @Override
    public String getName() {
        return LastActionTimePending.class.getName();
    }
}
