package com.latticeengines.pls.provisioning;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;

public class PLSDestroyer implements CustomerSpaceServiceDestroyer {

    private static final Log log = LogFactory.getLog(PLSDestroyer.class);

    private PLSComponentManager componentManager;

    @Override
    public boolean destroy(CustomerSpace space, String serviceName) {
        componentManager.discardTenant(space.toString());
        return true;
    }

    public void setComponentManager(PLSComponentManager manager) {
        this.componentManager = manager;
    }

}
