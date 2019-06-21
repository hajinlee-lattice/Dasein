package com.latticeengines.domain.exposed.cdl;

import java.util.Set;

public class TenantDuplicate implements Constraint {

    // TODO @Joy, consider using other way to deal with duplicate. then we can
    // remove Set<String> scheduledTenants from constraint interface, since you are
    // mutating SystemStatus anyways
    @Override
    public boolean checkViolated(SystemStatus currentState, Set<String> scheduledTenants, TenantActivity target) {
        if (scheduledTenants == null) {
            return true;
        }
        return scheduledTenants.contains(target.getTenantId());
    }

    @Override
    public String getName() {
        return TenantDuplicate.class.getName();
    }
}
