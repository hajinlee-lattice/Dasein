package com.latticeengines.metadata.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

public interface DataUnitEntityMgr {

    DataUnit createOrUpdateByNameAndStorageType(String tenantId, DataUnit dataUnit);

    List<DataUnit> findAll(String tenantId);

    List<DataUnit> findByNameFromReader(String tenantId, String name);

    List<DataUnit> findAllByTypeFromReader(String tenantId, DataUnit.StorageType storageType);

    DataUnit findByNameTypeFromReader(String tenantId, String name, DataUnit.StorageType storageType);

    void deleteByName(String tenantId, String name, DataUnit.StorageType storageType);

    void cleanupTenant(String tenantId);

    List<DataUnit> deleteAllByName(String tenantId, String name);

    DataUnit renameTableName(String tenantId, DataUnit dataUnit, String tableName);

    List<DataUnit> findByStorageType(DataUnit.StorageType storageType);

    void updateSignature(String tenantId, DataUnit dataUnit, String signature);

    List<DataUnit> findAllByDataTemplateIdAndRoleFromReader(String tenantId, String dataTemplateId, DataUnit.Role role);

    DataUnit findByDataTemplateIdAndRoleFromReader(String tenantId, String dataTemplateId, DataUnit.Role role);

    List<DataUnit> findAllDataUnitEntitiesWithExpiredRetentionPolicy(int pageIndex, int pageSize);

    DataUnit createOrUpdateByNameAndStorageType(String tenantId, DataUnit dataUnit, boolean purgeOldSnapShot);
}
