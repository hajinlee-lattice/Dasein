package com.latticeengines.metadata.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.RetentionPolicyUtil;
import com.latticeengines.metadata.service.DataUnitService;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.metadata.service.MetadataTableCleanupService;

@Component("metadataTableCleanupServiceImpl")
public class MetadataTableCleanupServiceImpl implements MetadataTableCleanupService {

    private static final Logger log = LoggerFactory.getLogger(MetadataTableCleanupServiceImpl.class);

    @Inject
    private MetadataService metadataService;

    @Inject
    private DataUnitService dataUnitService;

    @Value("${metadata.table.cleanup.size}")
    private int maxCleanupSize;

    private int batchSize = 2500;

    private int searchCount = 20;

    private int lastIndexMetaData;

    private int lastIndexDataUnit;

    private Map<String, DataUnit> tableNameToDataUnit;

    @Override
    public Boolean cleanup() {
        cleanupTables("metadata", lastIndexMetaData);
        cleanupTables("dataunit", lastIndexDataUnit);
        return true;
    }

    private void cleanupTables(String tableType, int lastIndex) {
        log.info(tableType + " cleanup task started for.");
        List<Table> tablesToDelete = new ArrayList<>();
        tableNameToDataUnit = new HashMap<>();
        // only query 50000 records per clean up job
        int count = 0;
        int preLastIndex = lastIndex;
        try (PerformanceTimer timer =
                     new PerformanceTimer("Metadata table cleanup task at step find tables to delete")) {
            while (count < searchCount) {
                List<Table> tables = getAllWithExpiredRetentionPolicyTables(tableType, lastIndex);
                int index = 0;
                for (Table table : tables) {
                    index++;
                    long expireTime = RetentionPolicyUtil.getExpireTimeByRetentionPolicyStr(table.getRetentionPolicy());
                    if (expireTime > 0) {
                        if (System.currentTimeMillis() > table.getUpdated().getTime() + expireTime) {
                            tablesToDelete.add(table);
                        }
                        if (tablesToDelete.size() >= maxCleanupSize) {
                            break;
                        }
                    }
                }
                count++;
                if (tablesToDelete.size() >= maxCleanupSize) {
                    lastIndex += index;
                    break;
                }
                if (tables.size() == batchSize) {
                    lastIndex += batchSize;
                } else {
                    lastIndex = 0;
                }
                if (preLastIndex == lastIndex) {
                    break;
                }
            }
        }
        try (PerformanceTimer timer = new PerformanceTimer("Metadata table cleanup task at step delete tables")) {
            if (CollectionUtils.isNotEmpty(tablesToDelete)) {
                tablesToDelete.forEach(table -> {
                    cleanupTable(tableType, table);
                });
                if (lastIndex >= tablesToDelete.size()) {
                    lastIndex -= tablesToDelete.size();
                } else {
                    lastIndex = 0;
                }
                log.info(String.format("Size of table needs to be deleted is %d and scan index is %d.", tablesToDelete.size(), lastIndex));
            } else {
                log.info(String.format("No tables needs to clean up after scan %d records and scan index is %d.", batchSize * searchCount, lastIndex));
            }
        }
        updateLastIndex(tableType, lastIndex);
    }

    private void cleanupTable(String tableType, Table table) {
        switch (tableType) {
            case "metadata":
                cleanupMetaDataTable(table);
                break;
            case "dataunit":
                cleanupDataUnit(tableNameToDataUnit.get(table.getName()));
                break;
            default:
                log.info("Unknown tableType to cleanup :" + tableType);
        }
    }

    private List<Table> getAllWithExpiredRetentionPolicyTables(String tableType, int lastIndex) {
        List<Table> tables = new ArrayList<>();
        switch (tableType) {
            case "metadata":
                tables = metadataService.findAllWithExpiredRetentionPolicy(lastIndex, batchSize);
                break;
            case "dataunit":
                tables = getDataUnitTables(lastIndex);
                break;
            default:
                log.info("Unknown tableType to cleanup :" + tableType);
        }
        return tables;
    }

    private void updateLastIndex(String tableType, int lastIndex) {
        switch (tableType) {
            case "metadata":
                lastIndexMetaData = lastIndex;
                break;
            case "dataunit":
                lastIndexDataUnit = lastIndex;
                break;
            default:
                log.info("Unknown tableType to cleanup :" + tableType);
        }
    }

    private List<Table> getDataUnitTables(int lastindex) {
        int pageIndex = lastindex/batchSize;
        List<Table>tables = new ArrayList<>();
        List<DataUnit> dataUnits = dataUnitService.findAllDataUnitEntitiesWithExpiredRetentionPolicy(pageIndex, batchSize);
        if (CollectionUtils.isNotEmpty(dataUnits)) {
            dataUnits.forEach(dataUnit -> {
                Table table = new Table();
                table.setName(dataUnit.getTenant() + "_" + dataUnit.getStorageType().name() + "_" + dataUnit.getName());
                table.setUpdated(dataUnit.getUpdated());
                table.setRetentionPolicy(dataUnit.getRetentionPolicy());
                tables.add(table);
                tableNameToDataUnit.put(table.getName(), dataUnit);
            });
        }
        return tables;
    }

    private void cleanupDataUnit(DataUnit dataUnit) {
        if (dataUnit == null)
            return;
        try {
            dataUnitService.delete(dataUnit);
        } catch (Exception ex) {
            log.error(String.format("Could not cleanup dataunit for tenant: %s, table name %s and type %s",
                    dataUnit.getTenant(), dataUnit.getName(), dataUnit.getDataFormat(), ex.getMessage()));
        }
    }

    private void cleanupMetaDataTable(Table table) {
        Tenant tenant = table.getTenant();
        try {
            MultiTenantContext.setTenant(tenant);
            CustomerSpace customerSpace = CustomerSpace.parse(table.getTenant().getId());
            MultiTenantContext.setTenant(table.getTenant());
            switch (table.getTableType()) {
                case DATATABLE:
                    metadataService.deleteTableAndCleanup(customerSpace, table.getName());
                    break;
                case IMPORTTABLE:
                    metadataService.deleteImportTableAndCleanup(customerSpace, table.getName());
                    break;
                default:
                    break;
            }
            MultiTenantContext.setTenant(null);
        } catch (Exception ex) {
            log.error(String.format("Could not cleanup table for tenant: %s, table name %s and type %s",
                    tenant == null ? "null" : tenant.getId(), table.getName(), table.getTableType(), ex.getMessage()));
        }
    }

}

