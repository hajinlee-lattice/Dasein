package com.latticeengines.admin.functionalframework;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;

public class TestLatticeComponentDestroyer implements CustomerSpaceServiceDestroyer {
    @Override
    public boolean destroy(CustomerSpace space, String serviceName) {
        return false;
    }
}
