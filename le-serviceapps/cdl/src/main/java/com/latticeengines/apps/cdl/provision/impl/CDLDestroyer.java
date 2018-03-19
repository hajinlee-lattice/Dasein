package com.latticeengines.apps.cdl.provision.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.apps.cdl.provision.CDLComponentManager;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;

public class CDLDestroyer implements CustomerSpaceServiceDestroyer {

    private static final Logger log = LoggerFactory.getLogger(CDLDestroyer.class);

    private CDLComponentManager componentManager;

    @Override
    public boolean destroy(CustomerSpace space, String serviceName) {
        componentManager.discardTenant(space.toString());
        return true;
    }

    public void setComponentManager(CDLComponentManager manager) {
        this.componentManager = manager;
    }

}
