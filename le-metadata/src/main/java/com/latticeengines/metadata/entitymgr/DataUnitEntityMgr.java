package com.latticeengines.metadata.entitymgr;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

public interface DataUnitEntityMgr {

    DataUnit createOrUpdateByName(String tenantId, DataUnit dataUnit);

    DataUnit findByNameFromReader(String tenantId, String name);

    void deleteByName(String tenantId, String name);

    void cleanupTenant(String tenantId);

}
