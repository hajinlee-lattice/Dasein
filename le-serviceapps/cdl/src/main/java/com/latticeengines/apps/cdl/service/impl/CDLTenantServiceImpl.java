package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.service.ApplicationTenantService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceapps.core.BootstrapRequest;


@Service("cdlTenantService")
public class CDLTenantServiceImpl implements ApplicationTenantService {

    private static final Logger log = LoggerFactory.getLogger(CDLTenantServiceImpl.class);

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    @Override
    public void bootstrap(BootstrapRequest request) {
        String tenantId = request.getTenantId();
        log.info("Going to bootstrap tenant " + tenantId + " for cdl.");

        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);

        log.info("Initializing data feed for " + tenantId);
        dataFeedService.getOrCreateDataFeed(customerSpace.toString());

        log.info("Cleanup all attr config for " + tenantId);
        attrConfigEntityMgr.cleanupTenant(tenantId);
    }

    @Override
    public void cleanup(String tenantId) {
        log.info("Going to clean up tenant " + tenantId + " from cdl.");
    }

}
