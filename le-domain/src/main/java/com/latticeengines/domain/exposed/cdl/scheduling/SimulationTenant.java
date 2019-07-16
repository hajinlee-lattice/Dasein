package com.latticeengines.domain.exposed.cdl.scheduling;

public class SimulationTenant {

    private  TenantActivity tenantActivity;
    public RandomConfig randomConfig;

    public SimulationTenant(TenantActivity tenantActivity) {
       this.tenantActivity = tenantActivity;
       this.randomConfig = new DefaultTenantPARunningTimeRadomConfig();
    }

    public SimulationTenant(TenantActivity tenantActivity, RandomConfig randomConfig) {
        this.tenantActivity = tenantActivity;
        this.randomConfig = randomConfig;
    }

    public TenantActivity getTenantActivity() {
        return tenantActivity;
    }

    public int getRandom() {
        return randomConfig.getRandom(tenantActivity);
    }

    public boolean isSucceed() {
        return randomConfig.isSucceed(tenantActivity);
    }
}
