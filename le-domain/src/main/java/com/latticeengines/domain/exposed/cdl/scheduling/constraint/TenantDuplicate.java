package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public class TenantDuplicate implements Constraint {

    @Override
    public ConstraintValidationResult validate(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        if (currentState.getScheduleTenants() == null) {
            return ConstraintValidationResult.VALID;
        }
        return new ConstraintValidationResult(currentState.getScheduleTenants().contains(target.getTenantId()), null);
    }

    @Override
    public String getName() {
        return TenantDuplicate.class.getName();
    }
}
