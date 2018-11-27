package com.latticeengines.admin.document.repository.reader;

import com.latticeengines.documentdb.entity.TenantConfigEntity;
import com.latticeengines.documentdb.repository.AdminDocumentRepository;

public interface TenantConfigEntityReaderRepository extends AdminDocumentRepository<TenantConfigEntity> {

    TenantConfigEntity findByContractIdAndTenantId(String contractId, String tenantId);
}
