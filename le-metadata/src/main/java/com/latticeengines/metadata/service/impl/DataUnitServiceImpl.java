package com.latticeengines.metadata.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.metadata.entitymgr.DataUnitEntityMgr;
import com.latticeengines.metadata.service.DataUnitService;

@Service("dataUnitService")
public class DataUnitServiceImpl implements DataUnitService {

    @Inject
    private DataUnitEntityMgr entityMgr;

    @Override
    public DataUnit createOrUpdateByName(DataUnit dataUnit) {
        String tenantId = MultiTenantContext.getTenantId();
        return entityMgr.createOrUpdateByName(tenantId, dataUnit);
    }

    @Override
    public List<DataUnit> findByNameFromReader(String name) {
        String tenantId = MultiTenantContext.getTenantId();
        return entityMgr.findByNameFromReader(tenantId, name);
    }

    @Override
    public void deleteByNameAndStorageType(String name, DataUnit.StorageType storageType) {
        String tenantId = MultiTenantContext.getTenantId();
        entityMgr.deleteByName(tenantId, name, storageType);
    }

}
