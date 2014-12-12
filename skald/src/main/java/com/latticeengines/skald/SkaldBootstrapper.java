package com.latticeengines.skald;

import com.latticeengines.camille.exposed.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.camille.exposed.config.bootstrap.Installer;
import com.latticeengines.camille.exposed.config.bootstrap.Upgrader;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class SkaldBootstrapper implements Installer, Upgrader {
    public static void register() {
        SkaldBootstrapper bootstrapper = new SkaldBootstrapper();
        CustomerSpaceServiceBootstrapManager.register(DocumentConstants.SERVICE_NAME, bootstrapper, bootstrapper);
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
