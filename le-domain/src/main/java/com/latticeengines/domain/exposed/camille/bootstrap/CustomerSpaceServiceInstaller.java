package com.latticeengines.domain.exposed.camille.bootstrap;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public interface CustomerSpaceServiceInstaller {
    
    DocumentDirectory install(CustomerSpace space, String serviceName, int dataVersion, Map<String, String> properties);
}
