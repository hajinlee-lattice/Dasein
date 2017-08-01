package com.latticeengines.testframework.exposed.batonadapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

/**
 * This is a dummy installer for functional tests, the true installer resides in
 * Dante project
 */
public class DanteInstaller extends LatticeComponentInstaller {

    private static final Logger logger = LoggerFactory.getLogger(DanteInstaller.class);

    public DanteInstaller() {
        super(DanteComponent.componentName);
    }

    @Override
    public DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName,
            int dataVersion, DocumentDirectory configDir) {
        logger.info("Install dante using test installer.");
        return configDir;
    }
}
