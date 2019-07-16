package com.latticeengines.domain.exposed.cdl.scheduling;

public interface RandomConfig {

    int getRandom(TenantActivity tenantActivity);

    /**
     * set tenant failure probability
     */
    boolean isSucceed(TenantActivity tenantActivity);
}
