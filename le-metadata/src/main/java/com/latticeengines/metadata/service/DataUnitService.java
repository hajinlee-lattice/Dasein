package com.latticeengines.metadata.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

public interface DataUnitService {

    DataUnit createOrUpdateByNameAndStorageType(DataUnit dataUnit);

    List<DataUnit> findByNameFromReader(String name);

    List<DataUnit> findAllByType(DataUnit.StorageType storageType);

    DataUnit findByNameTypeFromReader(String name, DataUnit.StorageType storageType);

    void deleteByNameAndStorageType(String name, DataUnit.StorageType storageType);

    boolean delete(DataUnit dataUnit);

    boolean renameTableName(DataUnit dataUnit, String tableName);

    boolean cleanupByTenant();
}
