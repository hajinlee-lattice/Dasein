package com.latticeengines.admin.service;

import java.util.Set;

import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;

public interface ServiceService {

    Set<String> getRegisteredServiceKeySet();
    
    SerializableDocumentDirectory getDefaultServiceConfig(String serviceName);
}
