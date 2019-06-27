package com.latticeengines.domain.exposed.cdl.scheduling;

public class TenantDuplicate implements Constraint {

    @Override
    public boolean checkViolated(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        if (currentState.getScheduleTenants() == null) {
            return true;
        }
        return currentState.getScheduleTenants().contains(target.getTenantId());
    }

    @Override
    public String getName() {
        return TenantDuplicate.class.getName();
    }
}
