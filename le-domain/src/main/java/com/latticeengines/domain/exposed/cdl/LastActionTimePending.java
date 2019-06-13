package com.latticeengines.domain.exposed.cdl;

import java.util.Date;

public class LastActionTimePending implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target) {
        if (target.getLastActionTime() == null || target.getLastActionTime() == 0L) {
            return false;
        }
        long currentTime = new Date().getTime();
        long lastMinute = (currentTime - target.getLastActionTime()) / 60000;
        return lastMinute >= 10;
    }
}
