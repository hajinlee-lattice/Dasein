package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public interface Constraint {

    /**
     * Take current system state,  tenantActivity we already plan to run PA .
     */
    ConstraintValidationResult validate(SystemStatus currentState, TenantActivity target, TimeClock timeClock);

    String getName();
}
