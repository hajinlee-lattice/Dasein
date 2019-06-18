package com.latticeengines.domain.exposed.cdl;

import java.util.Set;

public class TenantDuplicate implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, Set<String> scheduledTenants, TenantActivity target) {
        if (scheduledTenants == null) {
            return true;
        }
        return scheduledTenants.contains(target.getTenantId());
    }
}
