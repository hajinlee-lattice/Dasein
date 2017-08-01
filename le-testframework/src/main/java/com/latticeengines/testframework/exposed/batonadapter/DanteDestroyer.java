package com.latticeengines.testframework.exposed.batonadapter;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;

public class DanteDestroyer implements CustomerSpaceServiceDestroyer {
    @Override
    public boolean destroy(CustomerSpace space, String serviceName) {
        return false;
    }
}
