package com.latticeengines.apps.cdl.document.repository.writer;

import com.latticeengines.documentdb.entity.DanteConfigEntity;
import com.latticeengines.documentdb.repository.MultiTenantDocumentRepository;

public interface DanteConfigWriterRepository extends MultiTenantDocumentRepository<DanteConfigEntity> {
    DanteConfigEntity findByTenantId(String tenantId);
}
