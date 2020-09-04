package com.latticeengines.apps.cdl.document.repository.reader;


import java.util.List;

import com.latticeengines.documentdb.entity.DanteConfigEntity;
import com.latticeengines.documentdb.repository.MultiTenantDocumentRepository;

public interface DanteConfigReaderRepository extends MultiTenantDocumentRepository<DanteConfigEntity> {

    List<DanteConfigEntity> findByTenantId(String tenantId);

}
