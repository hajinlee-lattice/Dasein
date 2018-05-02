package com.latticeengines.metadata.repository.document;

import java.util.List;

import org.springframework.data.repository.NoRepositoryBean;

import com.latticeengines.documentdb.entity.DataUnitEntity;
import com.latticeengines.documentdb.repository.MultiTenantDocumentRepository;

@NoRepositoryBean
public interface DataUnitRepository extends MultiTenantDocumentRepository<DataUnitEntity> {

    List<DataUnitEntity> findByName(String name);

    List<DataUnitEntity> findByTenantIdAndName(String tenantId, String name);

    DataUnitEntity findByTenantIdAndNameAndStorageType(String tenantId, String name, String storageType);

}
