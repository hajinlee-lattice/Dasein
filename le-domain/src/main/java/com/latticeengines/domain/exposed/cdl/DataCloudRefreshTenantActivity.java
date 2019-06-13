package com.latticeengines.domain.exposed.cdl;

import com.latticeengines.domain.exposed.security.TenantType;

public class DataCloudRefreshTenantActivity extends TenantActivity {

    public DataCloudRefreshTenantActivity(TenantType tenantType) {
        this.tenantType = tenantType;
    }
}
