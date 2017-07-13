package com.latticeengines.pls.provisioning;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class PLSInstaller extends LatticeComponentInstaller {
    private static final Logger log = LoggerFactory.getLogger(PLSInstaller.class);

    private PLSComponentManager componentManager;

    public PLSInstaller() { super(PLSComponent.componentName); }

    @Override
    public DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir) {
        try {
            componentManager.provisionTenant(space, configDir);
            return configDir;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void setComponentManager(PLSComponentManager manager) {
        this.componentManager = manager;
    }

}
