package com.latticeengines.apps.lp.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.core.service.ApplicationTenantService;
import com.latticeengines.domain.exposed.serviceapps.core.BootstrapRequest;


@Service("lpTenantService")
public class LPTenantServiceImpl implements ApplicationTenantService {

    private static final Logger log = LoggerFactory.getLogger(LPTenantServiceImpl.class);

    @Override
    public void bootstrap(BootstrapRequest request) {
        String tenantId = request.getTenantId();
        log.info("Going to bootstrap tenant " + tenantId + " for lp.");
    }

    @Override
    public void cleanup(String tenantId) {
        log.info("Going to clean up tenant " + tenantId + " from lp.");
    }

}
