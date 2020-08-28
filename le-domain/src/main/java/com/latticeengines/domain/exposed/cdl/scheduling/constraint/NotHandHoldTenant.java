package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public class NotHandHoldTenant implements Constraint {

    @Override
    public ConstraintValidationResult validate(SystemStatus currentState, TenantActivity target,
            TimeClock timeClock) {
        boolean isHandHoldTenant = target.isHandHoldTenant();
        if (isHandHoldTenant) {
            return new ConstraintValidationResult(true, "this is a hand hold tenant");
        }
        return ConstraintValidationResult.VALID;
    }
}
