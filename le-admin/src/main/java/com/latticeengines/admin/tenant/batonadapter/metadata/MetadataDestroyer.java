package com.latticeengines.admin.tenant.batonadapter.metadata;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;

public class MetadataDestroyer implements CustomerSpaceServiceDestroyer {
    @Override
    public boolean destroy(CustomerSpace space, String serviceName) {
        return false;
    }
}
