package com.latticeengines.metadata.provisioning;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class MetadataInstaller extends LatticeComponentInstaller {
    private static final Log log = LogFactory.getLog(MetadataInstaller.class);

    private MetadataComponentManager componentManager;

    public MetadataInstaller() { super(MetadataComponent.componentName); }

    @Override
    public DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir) {
        try {
            componentManager.provisionTenant(space, configDir);
            return configDir;
        } catch (Exception e) {
            log.error(e);
            throw e;
        }
    }

    public void setComponentManager(MetadataComponentManager manager) {
        this.componentManager = manager;
    }

}
