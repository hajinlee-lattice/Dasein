package com.latticeengines.pls.provisioning;

import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class PLSInstaller extends LatticeComponentInstaller {

    private PLSComponentManager componentManager;

    public PLSInstaller() { super(PLSComponent.componentName); }

    @Override
    public void installCore(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir) {
        if (!serviceName.equals(PLSComponent.componentName)) { return; }
        componentManager.provisionTenant(space, configDir);
    }

    public void setComponentManager(PLSComponentManager manager) {
        this.componentManager = manager;
    }

}
