package com.latticeengines.metadata.provisioning;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;

public class MetadataDestroyer implements CustomerSpaceServiceDestroyer {

    private static final Logger log = LoggerFactory.getLogger(MetadataDestroyer.class);

    private MetadataComponentManager componentManager;

    @Override
    public boolean destroy(CustomerSpace space, String serviceName) {
        try {
            componentManager.purgeData(space);
            componentManager.removeImportTables(space);
        } catch (Exception e) {
            log.error(e.getMessage());
        }

        try {

            componentManager.removeImportTables(space);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return true;
    }

    public void setComponentManager(MetadataComponentManager manager) {
        this.componentManager = manager;
    }
}
