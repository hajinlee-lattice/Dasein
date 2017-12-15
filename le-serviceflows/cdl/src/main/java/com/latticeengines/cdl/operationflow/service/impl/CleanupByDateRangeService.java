package com.latticeengines.cdl.operationflow.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.cdl.operationflow.service.MaintenanceOperationService;
import com.latticeengines.domain.exposed.cdl.CleanupByDateRangeConfiguration;

@Component("cleanupByDateRangeService")
public class CleanupByDateRangeService extends MaintenanceOperationService<CleanupByDateRangeConfiguration> {
    @Override
    public void invoke(CleanupByDateRangeConfiguration config) {

    }
}
