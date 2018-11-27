package com.latticeengines.admin.document.repository.writer;

import com.latticeengines.documentdb.entity.ServiceConfigEntity;
import com.latticeengines.documentdb.entity.TenantConfigEntity;
import com.latticeengines.documentdb.repository.AdminDocumentRepository;

public interface ServiceConfigEntityWriterRepository extends AdminDocumentRepository<ServiceConfigEntity> {
    ServiceConfigEntity findByTenantConfigEntityAndServiceName(TenantConfigEntity entity, String serviceName);

}
