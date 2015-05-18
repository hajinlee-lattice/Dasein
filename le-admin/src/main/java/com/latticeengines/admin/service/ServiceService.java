package com.latticeengines.admin.service;

import java.util.Set;

import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public interface ServiceService {

    Set<String> getRegisteredServices();
    
    SerializableDocumentDirectory getDefaultServiceConfig(String serviceName);

    DocumentDirectory getConfigurationSchema(String serviceName);

    SelectableConfigurationDocument getSelectableConfigurationFields(String serviceName);

    Boolean patchOptions(String serviceName, SelectableConfigurationField field);
}
