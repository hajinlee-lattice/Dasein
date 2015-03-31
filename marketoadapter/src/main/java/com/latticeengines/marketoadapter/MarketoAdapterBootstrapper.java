package com.latticeengines.marketoadapter;

import com.latticeengines.camille.exposed.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

public class MarketoAdapterBootstrapper implements CustomerSpaceServiceInstaller, CustomerSpaceServiceUpgrader {
    // TODO Consider the best place for these constants to be declared.
    public static final String SERVICE_NAME = "MarketoAdapter";
    public static final int DATA_VERSION = 1;

    public static void register() {
        MarketoAdapterBootstrapper bootstrapper = new MarketoAdapterBootstrapper();
        CustomerSpaceServiceBootstrapManager.register(SERVICE_NAME, bootstrapper, bootstrapper);
    }

    @Override
    public DocumentDirectory upgrade(CustomerSpace space, String service, int sourceVersion, int targetVersion,
            DocumentDirectory source) {
        return source;
    }

    @Override
    public DocumentDirectory install(CustomerSpace space, String service, int dataVersion) {
        return new DocumentDirectory();
    }
}
