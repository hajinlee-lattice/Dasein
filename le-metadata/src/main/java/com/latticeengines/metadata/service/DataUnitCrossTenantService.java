package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

public interface DataUnitCrossTenantService {

    boolean cleanupByTenant(String customerSpace);

    boolean delete(String customerSpace, DataUnit dataUnit);

    void deleteByNameAndStorageType(String customerSpace, String name, DataUnit.StorageType storageType);

    DataUnit createOrUpdateByNameAndStorageType(String customerSpace, DataUnit dataUnit);
}
