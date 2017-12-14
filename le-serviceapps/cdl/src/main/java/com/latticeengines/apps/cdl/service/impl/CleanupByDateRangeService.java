package com.latticeengines.apps.cdl.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.MaintenanceOperationService;
import com.latticeengines.domain.exposed.cdl.CleanupByDateRangeConfiguration;

@Component("cleanupByDateRangeService")
public class CleanupByDateRangeService extends MaintenanceOperationService<CleanupByDateRangeConfiguration> {
    @Override
    public void invoke(CleanupByDateRangeConfiguration config) {

    }
}
