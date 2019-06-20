package com.latticeengines.domain.exposed.cdl;

import java.util.Set;

public interface Constraint {

    /**
     * Take current system state,  tenantActivity we already plan to run PA .
     * Return true if we run PA for this tenant, the constraint will be violated.
     */
    boolean checkViolated(SystemStatus currentState, Set<String> scheduledTenants, TenantActivity target);
    String getName();
}
