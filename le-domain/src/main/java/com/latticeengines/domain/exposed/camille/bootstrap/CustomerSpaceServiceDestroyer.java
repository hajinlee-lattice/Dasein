package com.latticeengines.domain.exposed.camille.bootstrap;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public interface CustomerSpaceServiceDestroyer {

    boolean destroy(CustomerSpace space, String serviceName);
}
