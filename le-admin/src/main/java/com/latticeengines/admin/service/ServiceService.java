package com.latticeengines.admin.service;

import java.util.Set;

import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public interface ServiceService {

    Set<String> getRegisteredServiceKeySet();
    
    SerializableDocumentDirectory getDefaultServiceConfig(String serviceName);

    DocumentDirectory getConfigurationSchema(String serviceName);
}
