package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;
import com.latticeengines.domain.exposed.security.TenantType;

public class FirstActionTimePending implements Constraint {
    @Override
    public ConstraintValidationResult validate(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        if (target.getFirstActionTime() == null || target.getFirstActionTime() == 0L) {
            return new ConstraintValidationResult(true, null);
        }
        long currentTime = timeClock.getCurrentTime();
        long hrSinceFirstAction = (currentTime - target.getFirstActionTime()) / 3600000;
        return new ConstraintValidationResult(
                !((target.getTenantType() == TenantType.CUSTOMER && hrSinceFirstAction >= 2)
                        || hrSinceFirstAction >= 6),
                "there are recent activities in this tenant");
    }

    @Override
    public String getName() {
        return FirstActionTimePending.class.getName();
    }
}
