package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public class DataCloudRefreshExist implements Constraint {
    @Override
    public ConstraintValidationResult validate(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        return new ConstraintValidationResult(!target.isDataCloudRefresh(), null);
    }

    @Override
    public String getName() {
        return DataCloudRefreshExist.class.getName();
    }
}
