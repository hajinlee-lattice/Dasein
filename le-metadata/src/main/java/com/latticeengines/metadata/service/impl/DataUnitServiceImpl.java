package com.latticeengines.metadata.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.metadata.entitymgr.DataUnitEntityMgr;
import com.latticeengines.metadata.service.DataUnitRuntimeService;
import com.latticeengines.metadata.service.DataUnitRuntimeServiceRegistry;
import com.latticeengines.metadata.service.DataUnitService;

@Service("dataUnitService")
public class DataUnitServiceImpl implements DataUnitService {

    private static final Logger log = LoggerFactory.getLogger(DataUnitServiceImpl.class);

    @Inject
    private DataUnitEntityMgr entityMgr;

    @Override
    public DataUnit createOrUpdateByNameAndStorageType(DataUnit dataUnit) {
        String tenantId = MultiTenantContext.getShortTenantId();
        return entityMgr.createOrUpdateByNameAndStorageType(tenantId, dataUnit);
    }

    @Override
    public List<DataUnit> findByNameFromReader(String name) {
        String tenantId = MultiTenantContext.getShortTenantId();
        return entityMgr.findByNameFromReader(tenantId, name);
    }

    @Override
    public List<DataUnit> findAllByType(DataUnit.StorageType storageType) {
        String tenantId = MultiTenantContext.getShortTenantId();
        return entityMgr.findAllByTypeFromReader(tenantId, storageType);
    }

    @Override
    public DataUnit findByNameTypeFromReader(String name, DataUnit.StorageType storageType) {
        String tenantId = MultiTenantContext.getShortTenantId();
        return entityMgr.findByNameTypeFromReader(tenantId, name, storageType);
    }

    @Override
    public DataUnit findByDataTemplateIdAndRole(String dataTemplateId, DataUnit.Role role) {
        String tenantId = MultiTenantContext.getShortTenantId();
        return entityMgr.findByDataTemplateIdAndRoleFromReader(tenantId, dataTemplateId, role);
    }
    @Override
    public void deleteByNameAndStorageType(String name, DataUnit.StorageType storageType) {
        String tenantId = MultiTenantContext.getShortTenantId();
        entityMgr.deleteByName(tenantId, name, storageType);
    }

    @Override
    public boolean delete(DataUnit dataUnit) {
        DataUnitRuntimeService dataUnitRuntimeService = DataUnitRuntimeServiceRegistry.getRunTimeService(dataUnit.getClass());
        if (dataUnitRuntimeService == null) {
            throw new RuntimeException(
                    String.format("Cannot find the dataUnit runtime service for dataUnit class: %s",
                            dataUnit.getClass()));
        }
        try {
            dataUnitRuntimeService.delete(dataUnit);
            deleteByTenantIdAndNameAndStorageType(dataUnit.getTenant(), dataUnit.getName(), dataUnit.getStorageType());
            return true;
        } catch (Exception e) {
            log.error(e.getMessage());
            return false;
        }
    }

    private void deleteByTenantIdAndNameAndStorageType(String tenantId, String name, DataUnit.StorageType storageType) {
        entityMgr.deleteByName(tenantId, name, storageType);
    }

    @Override
    public boolean renameTableName(DataUnit dataUnit, String tableName) {
        DataUnitRuntimeService dataUnitRuntimeService = DataUnitRuntimeServiceRegistry.getRunTimeService(dataUnit.getClass());
        if (dataUnitRuntimeService == null) {
            throw new RuntimeException(
                    String.format("Cannot find the dataUnit runtime service for dataUnit class: %s",
                            dataUnit.getClass()));
        }
        try {
            dataUnitRuntimeService.renameTableName(dataUnit, tableName);
            String tenantId = MultiTenantContext.getShortTenantId();
            entityMgr.renameTableName(tenantId, dataUnit, tableName);
            return true;
        } catch (Exception e) {
            log.error(e.getMessage());
            return false;
        }
    }

    @Override
    public boolean cleanupByTenant() {
        String tenantId = MultiTenantContext.getShortTenantId();
        List<DataUnit> dataUnits = entityMgr.findAll(tenantId);
        if (!CollectionUtils.isEmpty(dataUnits)) {
            for (DataUnit dataUnit : dataUnits) {
                try {
                    delete(dataUnit);
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }
        }
        return true;
    }

    @Override
    public List<DataUnit> findByStorageType(DataUnit.StorageType storageType) {
        return entityMgr.findByStorageType(storageType);
    }

    @Override
    public void updateSignature(DataUnit dataUnit, String signature) {
        String tenantId = MultiTenantContext.getShortTenantId();
        entityMgr.updateSignature(tenantId, dataUnit, signature);
    }

    @Override
    public List<DataUnit> findAllByDataTemplateIdAndRole(String dataTemplateId, DataUnit.Role role) {
        String tenantId = MultiTenantContext.getShortTenantId();
        return entityMgr.findAllByDataTemplateIdAndRoleFromReader(tenantId, dataTemplateId, role);
    }

    @Override
    public List<DataUnit> findAllDataUnitEntitiesWithExpiredRetentionPolicy(int pageIndex, int pageSize) {
        return entityMgr.findAllDataUnitEntitiesWithExpiredRetentionPolicy(pageIndex, pageSize);
    }

}
