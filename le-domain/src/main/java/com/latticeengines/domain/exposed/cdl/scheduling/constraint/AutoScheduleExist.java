package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public class AutoScheduleExist implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        return !target.isAutoSchedule();
    }

    @Override
    public String getName() {
        return AutoScheduleExist.class.getName();
    }
}
