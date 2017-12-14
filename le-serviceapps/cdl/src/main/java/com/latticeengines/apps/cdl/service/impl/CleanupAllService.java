package com.latticeengines.apps.cdl.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.MaintenanceOperationService;
import com.latticeengines.domain.exposed.cdl.CleanupAllConfiguration;

@Component("cleanupAllService")
public class CleanupAllService  extends MaintenanceOperationService<CleanupAllConfiguration> {
    @Override
    public void invoke(CleanupAllConfiguration config) {

    }
}
