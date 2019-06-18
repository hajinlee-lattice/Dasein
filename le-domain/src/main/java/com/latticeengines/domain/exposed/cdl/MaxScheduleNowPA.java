package com.latticeengines.domain.exposed.cdl;

import java.util.Set;

public class MaxScheduleNowPA implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, Set<String> scheduledTenants, TenantActivity target) {
        return currentState.getCanRunScheduleNowJobCount() < 1 && target.isScheduledNow();
    }
}
