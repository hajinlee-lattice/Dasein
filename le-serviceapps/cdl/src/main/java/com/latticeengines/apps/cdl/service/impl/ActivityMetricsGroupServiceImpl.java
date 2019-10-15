package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.entitymgr.ActivityMetricsGroupEntityMgr;
import com.latticeengines.apps.cdl.service.ActivityMetricsGroupService;
import com.latticeengines.common.exposed.workflow.annotation.WithCustomerSpace;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.security.Tenant;

@Service("activityMetricsGroupService")
public class ActivityMetricsGroupServiceImpl implements ActivityMetricsGroupService {

    @Inject
    private ActivityMetricsGroupEntityMgr activityMetricsGroupEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Override
    @WithCustomerSpace
    public ActivityMetricsGroup findByPid(String customerSpace, Long pid) {
        return activityMetricsGroupEntityMgr.findByPid(pid);
    }

    @Override
    @WithCustomerSpace
    public List<ActivityMetricsGroup> findByCustomerSpace(String customerSpace) {
        Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse(customerSpace).toString());
        if (tenant == null) {
            throw new RuntimeException(String.format("No tenant found with id %s", customerSpace));
        }
        return activityMetricsGroupEntityMgr.findByTenant(tenant);
    }
}
