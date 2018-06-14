package com.latticeengines.apps.cdl.provision.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.provision.CDLComponentManager;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.DataUnitEntityMgr;

@Component
public class CDLComponentManagerImpl implements CDLComponentManager {

    private static final Logger log = LoggerFactory.getLogger(CDLComponentManagerImpl.class);

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    @Inject
    private DataUnitEntityMgr dataUnitEntityMgr;

    @Inject
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    public void provisionTenant(CustomerSpace space, DocumentDirectory configDir) {
        // get tenant information
        String camilleTenantId = space.getTenantId();
        String camilleContractId = space.getContractId();
        String camilleSpaceId = space.getSpaceId();
        String customerSpace = String.format("%s.%s.%s", camilleContractId, camilleTenantId, camilleSpaceId);
        log.info(String.format("Provisioning tenant %s", customerSpace));
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        MultiTenantContext.setTenant(tenant);
        dataCollectionEntityMgr.createDefaultCollection();
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        log.info("Initialized data collection " + dataFeed.getDataCollection().getName());
    }

    public void discardTenant(String customerSpace) {
        String tenantId = CustomerSpace.parse(customerSpace).getTenantId();
        attrConfigEntityMgr.cleanupTenant(tenantId);
        dataUnitEntityMgr.cleanupTenant(tenantId);
    }

}
