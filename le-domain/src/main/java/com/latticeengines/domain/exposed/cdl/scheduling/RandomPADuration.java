package com.latticeengines.domain.exposed.cdl.scheduling;

public interface RandomPADuration {

    int getRandom(TenantActivity tenantActivity);

    /**
     * set tenant failure probability
     */
    boolean isSucceed(TenantActivity tenantActivity);
}
