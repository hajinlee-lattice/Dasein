package com.latticeengines.metadata.repository.document.reader;

import java.util.List;

import com.latticeengines.documentdb.entity.DataUnitEntity;
import com.latticeengines.documentdb.repository.CrossTenantDocumentRepository;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

public interface DataUnitCrossTenantReaderRepository extends CrossTenantDocumentRepository<DataUnitEntity> {

    List<DataUnitEntity> findByStorageTypeOrderByTenantId(DataUnit.StorageType storageType);

}
