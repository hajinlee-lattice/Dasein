package com.latticeengines.domain.exposed.cdl;

import java.util.Date;
import java.util.Set;

import com.latticeengines.domain.exposed.security.TenantType;

public class FirstActionTimePending implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, Set<String> scheduledTenants, TenantActivity target) {
        if (target.getFirstActionTime() == null || target.getFirstActionTime() == 0L) {
            return true;
        }
        long currentTime = new Date().getTime();
        long firstMinute = (currentTime - target.getFirstActionTime()) / 3600000;
        return !((target.getTenantType() == TenantType.CUSTOMER && firstMinute >= 2) || firstMinute >= 6);
    }
}
