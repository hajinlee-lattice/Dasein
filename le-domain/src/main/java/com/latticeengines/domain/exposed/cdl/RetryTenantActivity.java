package com.latticeengines.domain.exposed.cdl;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.security.TenantType;

public class RetryTenantActivity extends TenantActivity {

    @Override
    public TenantType getTenantType() {
        return null;
    }

    @Override
    public int compareTo(TenantActivity o) {
        int superResult = super.compareTo(o);
        if (superResult != 0) {
            return superResult;
        }
        return compare((RetryTenantActivity) o);
    }

    public int compare(RetryTenantActivity o) {
        return o.getLastFinishTime() - this.getLastFinishTime() > 0 ? -1 : 1;
    }

    public boolean isValid() {
        Long currentTime = new Date().getTime();
        currentTime = (currentTime - this.getLastFinishTime()) / 60000;
        return this.getLastFinishTime() != 0L && currentTime >= 15;
    }
}
