package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;
import com.latticeengines.domain.exposed.security.TenantType;

public class FirstActionTimePending implements Constraint {

    public static final long FIRSTACTION_PENDING_HOUR = 2L;

    @Override
    public ConstraintValidationResult validate(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        if (target.getFirstActionTime() == null || target.getFirstActionTime() == 0L) {
            return new ConstraintValidationResult(true, null);
        }
        long currentTime = timeClock.getCurrentTime();
        long hrSinceFirstAction = (currentTime - target.getFirstActionTime()) / 3600000;
        return new ConstraintValidationResult(
                !((target.getTenantType() == TenantType.CUSTOMER && hrSinceFirstAction >= FIRSTACTION_PENDING_HOUR)
                        || hrSinceFirstAction >= 6),
                "there are recent activities in this tenant");
    }

    @Override
    public String getName() {
        return FirstActionTimePending.class.getName();
    }
}
