package com.latticeengines.apps.cdl.provision.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.provision.CDLComponentManager;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;

@Component
public class CDLComponentManagerImpl implements CDLComponentManager {

    private static final Logger log = LoggerFactory.getLogger(CDLComponentManagerImpl.class);

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    public void provisionTenant(CustomerSpace space, DocumentDirectory configDir) {
        // get tenant information
        String camilleTenantId = space.getTenantId();
        String camilleContractId = space.getContractId();
        String camilleSpaceId = space.getSpaceId();
        String customerSpace = String.format("%s.%s.%s", camilleContractId, camilleTenantId, camilleSpaceId);
        log.info(String.format("Provisioning tenant %s", customerSpace));
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        log.info("Initialized data collection " + dataFeed.getDataCollection().getName()
                + " and data feed " + dataFeed.getName());
    }

    public void discardTenant(String tenantId) {
        attrConfigEntityMgr.cleanupTenant(CustomerSpace.parse(tenantId).getTenantId());
    }

}
