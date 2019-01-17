package com.latticeengines.metadata.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

public interface DataUnitEntityMgr {

    DataUnit createOrUpdateByNameAndStorageType(String tenantId, DataUnit dataUnit);

    List<DataUnit> findByNameFromReader(String tenantId, String name);

    List<DataUnit> findAllByTypeFromReader(String tenantId, DataUnit.StorageType storageType);

    DataUnit findByNameTypeFromReader(String tenantId, String name, DataUnit.StorageType storageType);

    void deleteByName(String tenantId, String name, DataUnit.StorageType storageType);

    void cleanupTenant(String tenantId);

    List<DataUnit> deleteAllByName(String name);

    DataUnit renameRedShiftTableName(String tenantId, DataUnit dataUnit, String tableName);

}
