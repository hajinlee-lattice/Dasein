package com.latticeengines.domain.exposed.camille.bootstrap;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public interface CustomerSpaceServiceUpgrader {
    
    DocumentDirectory upgrade(CustomerSpace space, String serviceName, int sourceVersion, int targetVersion,
            DocumentDirectory source, Map<String, String> properties);
}
