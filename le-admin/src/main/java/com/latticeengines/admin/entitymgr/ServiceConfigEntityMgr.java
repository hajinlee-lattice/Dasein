package com.latticeengines.admin.entitymgr;

import com.latticeengines.documentdb.entity.ServiceConfigEntity;
import com.latticeengines.documentdb.entity.TenantConfigEntity;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

public interface ServiceConfigEntityMgr {

    ServiceConfigEntity createServiceForTenant(TenantConfigEntity tenantConfig, String serviceName,
            SerializableDocumentDirectory serviceConfig, BootstrapState state);

    ServiceConfigEntity getTenantService(TenantConfigEntity entity, String serviceName);
}
