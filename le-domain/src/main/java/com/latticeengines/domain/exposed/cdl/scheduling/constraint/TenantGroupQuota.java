package com.latticeengines.domain.exposed.cdl.scheduling.constraint;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.cdl.scheduling.SystemStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.TimeClock;

public class TenantGroupQuota implements Constraint {
    @Override
    public ConstraintValidationResult validate(SystemStatus currentState, TenantActivity target, TimeClock timeClock) {
        if (MapUtils.isEmpty(currentState.getTenantGroups()) || StringUtils.isEmpty(target.getTenantId())) {
            return ConstraintValidationResult.VALID;
        }
        return new ConstraintValidationResult(currentState.getTenantGroups().values().stream() //
                .anyMatch(group -> group.reachQuotaLimit(target.getTenantId())), null);
    }

    @Override
    public String getName() {
        return TenantGroupQuota.class.getSimpleName();
    }
}
