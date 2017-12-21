package com.latticeengines.cdl.operationflow.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.operationflow.service.MaintenanceOperationService;
import com.latticeengines.domain.exposed.cdl.CleanupByDateRangeConfiguration;

@Component("cleanupByDateRangeService")
public class CleanupByDateRangeService extends MaintenanceOperationService<CleanupByDateRangeConfiguration> {

    private static Logger log = LoggerFactory.getLogger(CleanupByDateRangeService.class);

    @Override
    public void invoke(CleanupByDateRangeConfiguration config) {
        log.info("Start cleanup by date range operation!");
    }
}
