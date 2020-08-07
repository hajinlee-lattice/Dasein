package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public class MaxLargePA implements Constraint {

    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        return currentState.getCanRunLargeJobCount() < 1 && target.isLarge();
    }

    @Override
    public String getName() {
        return MaxLargePA.class.getName();
    }
}
