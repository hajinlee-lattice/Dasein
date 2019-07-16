package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.Random;

public class DefaultTenantPARunningTimeRadomConfig implements RandomConfig {
    @Override
    public int getRandom(TenantActivity tenantActivity) {
        Random r = new Random();
        if (tenantActivity.isLarge()) {
            return r.nextInt(5) + 4;
        } else {
            return r.nextInt(4) + 1;
        }
    }

    @Override
    public boolean isSucceed(TenantActivity tenantActivity) {
        Random r = new Random();
        int num = r.nextInt(100);
        if (tenantActivity.isLarge()) {
            return false;
        } else {
            return num >= 10;
        }
    }
}
