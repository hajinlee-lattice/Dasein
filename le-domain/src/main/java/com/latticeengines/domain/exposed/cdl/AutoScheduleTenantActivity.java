package com.latticeengines.domain.exposed.cdl;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.security.TenantType;

public class AutoScheduleTenantActivity extends TenantActivity {

    @Override
    public int compareTo(TenantActivity o) {
        return compare((AutoScheduleTenantActivity) o);
    }

    public int compare(AutoScheduleTenantActivity o) {
        return o.getInvokeTime() != null && o.getInvokeTime() - this.getInvokeTime() > 0 ? -1 : 1;
    }

    public AutoScheduleTenantActivity(TenantType tenantType) {
        this.tenantType = tenantType;
    }

    public boolean isValid() {
        if (this.getLastActionTime() == null || this.getLastActionTime() == 0L || this.getFirstActionTime() == null || this.getFirstActionTime() == 0L) {
            return false;
        }
        Long currentTime = new Date().getTime();
        long lastMinute = (currentTime - this.getLastActionTime()) / 60000;
        long firstMinute = (currentTime - this.getFirstActionTime()) / 3600000;
        return lastMinute >= 10 && ((tenantType == TenantType.CUSTOMER && firstMinute >= 2) || firstMinute >= 6);
    }
}
