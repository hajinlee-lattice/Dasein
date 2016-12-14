package com.latticeengines.admin.tenant.batonadapter.template.dl;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;

public class DLTemplateDestroyer implements CustomerSpaceServiceDestroyer {
    @Override
    public boolean destroy(CustomerSpace space, String serviceName) {
        return false;
    }
}
