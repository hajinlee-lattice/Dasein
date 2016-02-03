package com.latticeengines.scoringapi.infrastructure;

import java.util.Map;

import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceProperties;
import com.latticeengines.scoringapi.unused.DocumentConstants;

public class ScoringCamilleBootstrapper implements CustomerSpaceServiceInstaller, CustomerSpaceServiceUpgrader {
    public static void register() {
        ScoringCamilleBootstrapper bootstrapper = new ScoringCamilleBootstrapper();
        ServiceInfo info = new ServiceInfo(new ServiceProperties(DocumentConstants.DATA_VERSION), bootstrapper,
                bootstrapper, null);
        ServiceWarden.registerService(DocumentConstants.SERVICE_NAME, info);
    }

    @Override
    public DocumentDirectory upgrade(CustomerSpace space, String service, int sourceVersion, int targetVersion,
            DocumentDirectory source, Map<String, String> properties) {
        return source;
    }

    @Override
    public DocumentDirectory install(CustomerSpace space, String service, int dataVersion,
            Map<String, String> properties) {
        // TODO Probably worth installing empty defaults for necessary files.

        return new DocumentDirectory();
    }
}
