package com.latticeengines.domain.exposed.cdl.scheduling;

public class SimulationTenant {

    private  TenantActivity tenantActivity;
    public RandomPADuration randomPADuration;

    public SimulationTenant(TenantActivity tenantActivity) {
       this.tenantActivity = tenantActivity;
       this.randomPADuration = new DefaultTenantPARunningTimeRadomPADuration();
    }

    public SimulationTenant(TenantActivity tenantActivity, RandomPADuration randomPADuration) {
        this.tenantActivity = tenantActivity;
        this.randomPADuration = randomPADuration;
    }

    public TenantActivity getTenantActivity() {
        return tenantActivity;
    }

    public int getRandom() {
        return randomPADuration.getRandom(tenantActivity);
    }

    public boolean isSucceed() {
        return randomPADuration.isSucceed(tenantActivity);
    }
}
