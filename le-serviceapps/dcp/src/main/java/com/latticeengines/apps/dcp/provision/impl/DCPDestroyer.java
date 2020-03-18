package com.latticeengines.apps.dcp.provision.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.apps.dcp.provision.DCPComponentManager;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;

public class DCPDestroyer implements CustomerSpaceServiceDestroyer {

    private static final Logger log = LoggerFactory.getLogger(DCPDestroyer.class);

    private DCPComponentManager componentManager;

    @Override
    public boolean destroy(CustomerSpace space, String serviceName) {
        componentManager.discardTenant(space.toString());
        return true;
    }

    public void setComponentManager(DCPComponentManager manager) {
        this.componentManager = manager;
    }

}
