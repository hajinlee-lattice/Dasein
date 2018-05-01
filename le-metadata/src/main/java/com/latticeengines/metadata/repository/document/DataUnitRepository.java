package com.latticeengines.metadata.repository.document;

import org.springframework.data.repository.NoRepositoryBean;

import com.latticeengines.documentdb.entity.DataUnitEntity;
import com.latticeengines.documentdb.repository.MultiTenantDocumentRepository;

@NoRepositoryBean
public interface DataUnitRepository extends MultiTenantDocumentRepository<DataUnitEntity> {

    DataUnitEntity findByTenantIdAndName(String tenantId, String name);

}
