package com.latticeengines.admin.tenant.batonadapter.pls;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

public class PLSUpgrader implements CustomerSpaceServiceUpgrader {

    @Override
    public DocumentDirectory upgrade(CustomerSpace space, String serviceName, int sourceVersion, int targetVersion,
            DocumentDirectory source) {
        return null;
    }

}
