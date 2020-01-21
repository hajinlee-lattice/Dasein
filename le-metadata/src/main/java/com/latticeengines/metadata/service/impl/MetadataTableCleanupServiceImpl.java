package com.latticeengines.metadata.service.impl;

import java.util.ArrayList;
import java.util.List;

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
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.RetentionPolicyUtil;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.metadata.service.MetadataTableCleanupService;

@Component("metadataTableCleanupServiceImpl")
public class MetadataTableCleanupServiceImpl implements MetadataTableCleanupService {

    private static final Logger log = LoggerFactory.getLogger(MetadataTableCleanupServiceImpl.class);

    @Inject
    private MetadataService metadataService;

    @Value("${metadata.table.cleanup.size}")
    private int maxCleanupSize;

    private int batchSize = 2500;

    private int searchCount = 20;

    private int lastIndex;

    @Override
    public Boolean cleanup() {
        log.info("Metadata table cleanup task started.");
        List<Table> tablesToDelete = new ArrayList<>();
        // only query 50000 records per clean up job
        int count = 0;
        int preLastIndex = lastIndex;
        try (PerformanceTimer timer =
                     new PerformanceTimer("Metadata table cleanup task at step find tables to delete")) {
            while (count < searchCount) {
                List<Table> tables = metadataService.findAllWithExpiredRetentionPolicy(lastIndex, batchSize);
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
                    cleanupTable(table);
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
        return true;
    }

    private void cleanupTable(Table table) {
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

