package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public interface Constraint {

    /**
     * Take current system state,  tenantActivity we already plan to run PA .
     * Return true if we run PA for this tenant, the constraint will be violated.
     */
    boolean checkViolated(SystemStatus currentState, TenantActivity target, TimeClock timeClock);

    String getName();
}
