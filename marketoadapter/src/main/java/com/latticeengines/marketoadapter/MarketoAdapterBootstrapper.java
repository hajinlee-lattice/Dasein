package com.latticeengines.marketoadapter;

import com.latticeengines.camille.exposed.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.camille.exposed.config.bootstrap.Installer;
import com.latticeengines.camille.exposed.config.bootstrap.Upgrader;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class MarketoAdapterBootstrapper implements Installer, Upgrader {
    // TODO Consider the best place for these constants to be declared.
    public static final String SERVICE_NAME = "MarketoAdapter";
    public static final int DATA_VERSION = 1;

    public static void register() {
        MarketoAdapterBootstrapper bootstrapper = new MarketoAdapterBootstrapper();
        CustomerSpaceServiceBootstrapManager.register(SERVICE_NAME, bootstrapper, bootstrapper);
    }

    @Override
    public DocumentDirectory upgradeConfiguration(int sourceVersion, int targetVersion, DocumentDirectory source) {
        return source;
    }

    @Override
    public DocumentDirectory getInitialConfiguration(int dataVersion) {
        return new DocumentDirectory();
    }
}
