package com.latticeengines.domain.exposed.cdl;

import java.util.Set;

public class DataCloudRefreshExist implements Constraint {
    @Override
    public boolean checkViolated(SystemStatus currentState, Set<String> scheduledTenants, TenantActivity target) {
        return !target.isDataCloudRefresh();
    }

    @Override
    public String getName() {
        return DataCloudRefreshExist.class.getName();
    }
}
