package com.latticeengines.domain.exposed.cdl;

public class DataCloudRefreshExist implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target) {
        return !target.isDataCloudRefresh();
    }

    @Override
    public String getName() {
        return DataCloudRefreshExist.class.getName();
    }
}
