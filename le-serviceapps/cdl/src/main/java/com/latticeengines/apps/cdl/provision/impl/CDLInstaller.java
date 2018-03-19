package com.latticeengines.apps.cdl.provision.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.apps.cdl.provision.CDLComponentManager;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class CDLInstaller extends LatticeComponentInstaller {
    private static final Logger log = LoggerFactory.getLogger(CDLInstaller.class);

    private CDLComponentManager componentManager;

    public CDLInstaller() { super(CDLComponent.componentName); }

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

    public void setComponentManager(CDLComponentManager manager) {
        this.componentManager = manager;
    }

}
