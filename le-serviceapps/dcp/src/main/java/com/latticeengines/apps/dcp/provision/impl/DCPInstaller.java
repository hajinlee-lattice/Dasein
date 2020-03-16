package com.latticeengines.apps.dcp.provision.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.apps.dcp.provision.DCPComponentManager;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class DCPInstaller extends LatticeComponentInstaller {
    private static final Logger log = LoggerFactory.getLogger(DCPInstaller.class);

    private DCPComponentManager componentManager;

    public DCPInstaller() { super(DCPComponent.componentName); }

    @Override
    public DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName,
                                                                int dataVersion, DocumentDirectory configDir) {
        try {
            componentManager.provisionTenant(space, configDir);
            return configDir;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void setComponentManager(DCPComponentManager manager) {
        this.componentManager = manager;
    }

}
