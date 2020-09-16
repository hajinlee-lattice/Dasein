
package com.latticeengines.apps.cdl.document.repository.reader;

import com.latticeengines.documentdb.entity.DanteConfigEntity;
import com.latticeengines.documentdb.repository.MultiTenantDocumentRepository;

public interface DanteConfigReaderRepository extends MultiTenantDocumentRepository<DanteConfigEntity> {

    DanteConfigEntity findByTenantId(String tenantId);

}
