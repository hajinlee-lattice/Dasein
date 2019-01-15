package com.latticeengines.metadata.repository.document;

import java.util.List;

import org.springframework.data.repository.NoRepositoryBean;

import com.latticeengines.documentdb.entity.DataUnitEntity;
import com.latticeengines.documentdb.repository.MultiTenantDocumentRepository;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

@NoRepositoryBean
public interface DataUnitRepository extends MultiTenantDocumentRepository<DataUnitEntity> {

    List<DataUnitEntity> findByName(String name);

    List<DataUnitEntity> findByTenantIdAndStorageType(String tenantId, DataUnit.StorageType storageType);

    List<DataUnitEntity> findByTenantIdAndName(String tenantId, String name);

    DataUnitEntity findByTenantIdAndNameAndStorageType(String tenantId, String name, DataUnit.StorageType storageType);

}
