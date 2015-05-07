package com.latticeengines.admin.service;

import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public interface ServiceService {

    Set<String> getRegisteredServices();
    
    SerializableDocumentDirectory getDefaultServiceConfig(String serviceName);

    DocumentDirectory getConfigurationSchema(String serviceName);

    List<SelectableConfigurationField> getSelectableConfigurationFields(String serviceName);
}
