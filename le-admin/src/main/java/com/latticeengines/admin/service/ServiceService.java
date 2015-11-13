package com.latticeengines.admin.service;

import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public interface ServiceService {

    Set<String> getRegisteredServices();

    Map<String, Set<LatticeProduct>> getRegisteredServicesWithProducts();

    SerializableDocumentDirectory getDefaultServiceConfig(String serviceName);

    DocumentDirectory getConfigurationSchema(String serviceName);

    SelectableConfigurationDocument getSelectableConfigurationFields(String serviceName, boolean includeDynamicOpts);

    Boolean patchOptions(String serviceName, SelectableConfigurationField field);

    Boolean patchDefaultConfigWithOptions(String serviceName, SelectableConfigurationField field);

    Boolean patchDefaultConfig(String serviceName, String nodePath, String data);
}
