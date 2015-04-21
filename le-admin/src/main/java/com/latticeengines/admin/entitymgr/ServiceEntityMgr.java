package com.latticeengines.admin.entitymgr;

import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public interface ServiceEntityMgr {

    SerializableDocumentDirectory getDefaultServiceConfig(String serviceName);

    DocumentDirectory getConfigurationSchema(String serviceName);

}
