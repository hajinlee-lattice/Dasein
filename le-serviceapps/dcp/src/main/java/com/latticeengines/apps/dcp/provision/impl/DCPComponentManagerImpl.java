package com.latticeengines.apps.dcp.provision.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.dcp.provision.DCPComponentManager;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

@Component("dcpComponentManager")
public class DCPComponentManagerImpl implements DCPComponentManager {

    private static final Logger log = LoggerFactory.getLogger(DCPComponentManagerImpl.class);


    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private BatonService batonService;

    @Override
    public void provisionTenant(CustomerSpace space, DocumentDirectory configDir) {
        // get tenant information
        String camilleTenantId = space.getTenantId();
        String camilleContractId = space.getContractId();
        String camilleSpaceId = space.getSpaceId();
        String customerSpace = String.format("%s.%s.%s", camilleContractId, camilleTenantId, camilleSpaceId);
        log.info("Provisioning dcp component for tenant {}", customerSpace);


    }

    @Override
    public void discardTenant(String customerSpace) {
    }

}
