package com.latticeengines.admin.tenant.batonadapter.dataloader;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

public class DataLoaderUpgrader implements CustomerSpaceServiceUpgrader {

    @Override
    public DocumentDirectory upgrade(CustomerSpace space, String serviceName, int sourceVersion, int targetVersion,
            DocumentDirectory source) {
        return null;
    }


}
