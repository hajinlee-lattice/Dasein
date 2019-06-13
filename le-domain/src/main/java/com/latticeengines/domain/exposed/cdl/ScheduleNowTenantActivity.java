package com.latticeengines.domain.exposed.cdl;

import com.latticeengines.domain.exposed.security.TenantType;

public class ScheduleNowTenantActivity extends TenantActivity {

    @Override
    public int compareTo(TenantActivity o) {
        int superResult = super.compareTo(o);
        if (superResult != 0) {
            return superResult;
        }
        return compare((ScheduleNowTenantActivity) o);
    }

    public int compare(ScheduleNowTenantActivity o) {
        return o.getScheduleTime() - this.getScheduleTime() > 0 ? -1 : 1;
    }

    public ScheduleNowTenantActivity(TenantType tenantType) {
        this.tenantType = tenantType;
    }

}
