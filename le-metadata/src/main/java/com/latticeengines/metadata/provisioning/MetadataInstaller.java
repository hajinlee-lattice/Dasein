package com.latticeengines.metadata.provisioning;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class MetadataInstaller extends LatticeComponentInstaller {
    private static final Logger log = LoggerFactory.getLogger(MetadataInstaller.class);

    private MetadataComponentManager componentManager;

    public MetadataInstaller() { super(MetadataComponent.componentName); }

    @Override
    public DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir) {
        try {
            componentManager.provisionImportTables(space, configDir);
            return configDir;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void setComponentManager(MetadataComponentManager manager) {
        this.componentManager = manager;
    }

}
