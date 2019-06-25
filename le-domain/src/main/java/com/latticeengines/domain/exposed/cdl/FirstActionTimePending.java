package com.latticeengines.domain.exposed.cdl;

import java.util.Date;

import com.latticeengines.domain.exposed.security.TenantType;

public class FirstActionTimePending implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target) {
        if (target.getFirstActionTime() == null || target.getFirstActionTime() == 0L) {
            return true;
        }
        long currentTime = new Date().getTime();
        long firstMinute = (currentTime - target.getFirstActionTime()) / 3600000;
        return !((target.getTenantType() == TenantType.CUSTOMER && firstMinute >= 2) || firstMinute >= 6);
    }

    @Override
    public String getName() {
        return FirstActionTimePending.class.getName();
    }
}
