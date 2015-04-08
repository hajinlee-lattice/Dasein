package com.latticeengines.skald;

import java.util.Map;

import com.latticeengines.camille.exposed.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

public class SkaldBootstrapper implements CustomerSpaceServiceInstaller, CustomerSpaceServiceUpgrader {
    public static void register() {
        SkaldBootstrapper bootstrapper = new SkaldBootstrapper();
        CustomerSpaceServiceBootstrapManager.register(DocumentConstants.SERVICE_NAME, bootstrapper, bootstrapper);
    }

    @Override
    public DocumentDirectory upgrade(CustomerSpace space, String service, int sourceVersion, int targetVersion,
            DocumentDirectory source, Map<String, String> properties) {
        return source;
    }

    @Override
    public DocumentDirectory install(CustomerSpace space, String service, int dataVersion, Map<String, String> properties) {
        // TODO Probably worth installing empty defaults for necessary files.

        return new DocumentDirectory();
    }
}
