package com.latticeengines.cdl.operationflow.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.operationflow.service.MaintenanceOperationService;
import com.latticeengines.domain.exposed.cdl.CleanupAllConfiguration;

@Component("cleanupAllService")
public class CleanupAllService  extends MaintenanceOperationService<CleanupAllConfiguration> {

    private static Logger log = LoggerFactory.getLogger(CleanupAllService.class);

    @Override
    public void invoke(CleanupAllConfiguration config) {
        log.info("Start cleanup all operation!");
    }
}
