package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import org.apache.commons.collections4.SetUtils;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public class HasPAQuota implements Constraint {
    private final String quotaName;
    private final String quotaDisplayName;

    public HasPAQuota(String quotaName, String quotaDisplayName) {
        this.quotaName = quotaName;
        this.quotaDisplayName = quotaDisplayName;
    }

    @Override
    public ConstraintValidationResult validate(SystemStatus currentState, TenantActivity target,
            TimeClock timeClock) {
        boolean hasQuota = SetUtils.emptyIfNull(target.getNotExceededQuotaNames()).contains(quotaName);
        if (hasQuota) {
            return ConstraintValidationResult.VALID;
        }

        String msg = String.format("PA Quota [%s] has been exceeded", quotaDisplayName);
        return new ConstraintValidationResult(true, msg);
    }
}
