package com.latticeengines.domain.exposed.cdl;

import java.util.Set;

public class ScheduleNowExist implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, Set<String> scheduledTenants, TenantActivity target) {
        return !target.isScheduledNow();
    }

    @Override
    public String getName() {
        return ScheduleNowExist.class.getName();
    }
}
