package com.latticeengines.domain.exposed.cdl.scheduling;

public class DataCloudRefreshExist implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        return !target.isDataCloudRefresh();
    }

    @Override
    public String getName() {
        return DataCloudRefreshExist.class.getName();
    }
}
