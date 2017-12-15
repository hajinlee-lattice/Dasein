package com.latticeengines.cdl.operationflow.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.cdl.operationflow.service.MaintenanceOperationService;
import com.latticeengines.domain.exposed.cdl.CleanupAllConfiguration;

@Component("cleanupAllService")
public class CleanupAllService  extends MaintenanceOperationService<CleanupAllConfiguration> {
    @Override
    public void invoke(CleanupAllConfiguration config) {

    }
}
