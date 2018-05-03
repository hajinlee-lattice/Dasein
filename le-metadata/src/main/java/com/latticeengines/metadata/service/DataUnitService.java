package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

import java.util.List;

public interface DataUnitService {

    DataUnit createOrUpdateByNameAndStorageType(DataUnit dataUnit);

    List<DataUnit> findByNameFromReader(String name);

    void deleteByNameAndStorageType(String name, DataUnit.StorageType storageType);

}
