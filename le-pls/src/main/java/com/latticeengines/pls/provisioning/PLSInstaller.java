package com.latticeengines.pls.provisioning;

import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class PLSInstaller extends LatticeComponentInstaller {

    private PLSComponentManager componentManager;

    public PLSInstaller() { super(PLSComponent.componentName); }

    @Override
    public DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir) {
        componentManager.provisionTenant(space, configDir);
        return configDir;
    }

    public void setComponentManager(PLSComponentManager manager) {
        this.componentManager = manager;
    }

}
