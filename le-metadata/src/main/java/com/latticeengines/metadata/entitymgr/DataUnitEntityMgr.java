package com.latticeengines.metadata.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

public interface DataUnitEntityMgr {

    DataUnit createOrUpdateByName(String tenantId, DataUnit dataUnit);

    List<DataUnit> findByNameFromReader(String tenantId, String name);

    void deleteByName(String tenantId, String name, DataUnit.StorageType storageType);

    void cleanupTenant(String tenantId);

    List<DataUnit> deleteAllByName(String name);

}
