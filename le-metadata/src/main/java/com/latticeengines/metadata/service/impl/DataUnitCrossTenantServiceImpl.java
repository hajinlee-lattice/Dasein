package com.latticeengines.metadata.service.impl;

import java.util.List;

import javax.inject.Inject;

import com.latticeengines.metadata.service.DataUnitRuntimeServiceRegistry;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.metadata.entitymgr.DataUnitEntityMgr;
import com.latticeengines.metadata.service.DataUnitRuntimeService;

@Component("dataUnitCrossTenantService")
public class DataUnitCrossTenantServiceImpl implements com.latticeengines.metadata.service.DataUnitCrossTenantService {

    private static final Logger log = LoggerFactory.getLogger(DataUnitCrossTenantServiceImpl.class);

    @Inject
    private DataUnitEntityMgr entityMgr;

    @Override
    public boolean cleanupByTenant(String customerSpace) {
        String tenantId = CustomerSpace.parse(customerSpace).getTenantId();
        List<DataUnit> dataUnits = entityMgr.findAll(tenantId);
        if (!CollectionUtils.isEmpty(dataUnits)) {
            for (DataUnit dataUnit : dataUnits) {
                try {
                    delete(customerSpace, dataUnit);
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }
        }
        return true;
    }

    @Override
    public boolean delete(String customerSpace, DataUnit dataUnit) {
        DataUnitRuntimeService dataUnitRuntimeService = DataUnitRuntimeServiceRegistry.getRunTimeService(dataUnit.getClass());
        if (dataUnitRuntimeService == null) {
            throw new RuntimeException(
                    String.format("Cannot find the dataUnit runtime service for dataUnit class: %s",
                            dataUnit.getClass()));
        }
        try {
            dataUnitRuntimeService.delete(dataUnit);
            deleteByNameAndStorageType(customerSpace, dataUnit.getName(), dataUnit.getStorageType());
            return true;
        } catch (Exception e) {
            log.error(e.getMessage());
            return false;
        }
    }

    @Override
    public void deleteByNameAndStorageType(String customerSpace, String name, DataUnit.StorageType storageType) {
        String tenantId = CustomerSpace.parse(customerSpace).getTenantId();
        entityMgr.deleteByName(tenantId, name, storageType);
    }

    @Override
    public DataUnit createOrUpdateByNameAndStorageType(String customerSpace, DataUnit dataUnit) {
        String tenantId = CustomerSpace.parse(customerSpace).getTenantId();
        return entityMgr.createOrUpdateByNameAndStorageType(tenantId, dataUnit);
    }
}
