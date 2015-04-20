package com.latticeengines.admin.entitymgr;

import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;

public interface ServiceEntityMgr {

    SerializableDocumentDirectory getDefaultServiceConfig(String serviceName);

}
