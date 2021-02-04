package com.latticeengines.metadata.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.datastore.AthenaDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

public interface DataUnitService {

    DataUnit findByDataTemplateIdAndRole(String dataTemplateId, DataUnit.Role role);

    DataUnit createOrUpdateByNameAndStorageType(DataUnit dataUnit);

    List<DataUnit> findByNameFromReader(String name);

    List<DataUnit> findAllByType(DataUnit.StorageType storageType);

    DataUnit findByNameTypeFromReader(String name, DataUnit.StorageType storageType);

    void deleteByNameAndStorageType(String name, DataUnit.StorageType storageType);

    boolean delete(DataUnit dataUnit);

    boolean delete(String name, DataUnit.StorageType type);

    boolean renameTableName(DataUnit dataUnit, String tableName);

    boolean cleanupByTenant();

    List<DataUnit> findByStorageType(DataUnit.StorageType storageType);

    void updateSignature(DataUnit dataUnit, String signature);

    List<DataUnit> findAllByDataTemplateIdAndRole(String dataTemplateId, DataUnit.Role role);

    List<DataUnit> findAllDataUnitEntitiesWithExpiredRetentionPolicy(int pageIndex, int pageSize);

    AthenaDataUnit registerAthenaDataUnit(String dataUnitName);

    DataUnit createOrUpdateByNameAndStorageType(DataUnit dataUnit, boolean purgeOldSnapShot);
}
