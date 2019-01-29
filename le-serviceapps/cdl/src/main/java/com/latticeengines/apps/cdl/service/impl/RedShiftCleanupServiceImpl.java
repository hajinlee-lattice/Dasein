package com.latticeengines.apps.cdl.service.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataFeedEntityMgr;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.RedShiftCleanupService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

@Component("redShiftCleanupService")
public class RedShiftCleanupServiceImpl implements RedShiftCleanupService {

    private static final Logger log = LoggerFactory.getLogger(RedShiftCleanupServiceImpl.class);

    @Inject
    private RedshiftService redshiftService;

    @Inject
    private DataFeedEntityMgr dataFeedEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private DataUnitProxy dataUnitProxy;

    private final static String TABLE_PREFIX = "ToBeDeletedOn_";

    @Override
    public boolean removeUnusedTable() {
        List<SimpleDataFeed> allSimpleDataFeeds = dataFeedEntityMgr.getAllSimpleDataFeeds();
        if (CollectionUtils.isEmpty(allSimpleDataFeeds)) {
            return true;
        }
        for (SimpleDataFeed simpleDataFeed : allSimpleDataFeeds) {
            Tenant tenant = simpleDataFeed.getTenant();
            this.dropTableByTenant(tenant);
        }
        return true;
    }

    public boolean removeUnusedTableByTenant(String customerSpace) {
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        log.info("tenant is :" + tenant.getName());
        this.dropTableByTenant(tenant);
        return true;
    }

    private void dropTableByTenant(Tenant tenant) {
        try {
            MultiTenantContext.setTenant(tenant);
            log.info("tenant name is : " + tenant.getName());
            String customerspace = MultiTenantContext.getCustomerSpace().toString();
            DataCollection dataCollection = dataCollectionService.getDefaultCollection(customerspace);
            DataCollection.Version activeVersion = dataCollection.getVersion();
            DataCollection.Version inactiveVersion = activeVersion.complement();
            // 1. get all inused tablename in this tenant_id
            Set<String> tableNames = new HashSet<>();
            tableNames.addAll(dataCollectionService.getTableNames(customerspace, dataCollection.getName(), null, activeVersion));
            tableNames.addAll(dataCollectionService.getTableNames(customerspace, dataCollection.getName(), null, inactiveVersion));
            log.info("inuse tableNames:" + tableNames.toString());
            // 2.drop table
            cleanupTable(tenant, new ArrayList<>(tableNames));
        } catch (Exception e) {
            log.error(e.toString());
        }
    }

    private void cleanupTable(Tenant tenant, List<String> inuseTableName) {
        List<DataUnit> dataUnits = dataUnitProxy.getByStorageType(tenant.getId(), DataUnit.StorageType.Redshift);
        for (DataUnit dataUnit : dataUnits) {
            log.info("dataUnit = " + dataUnit.getName());
            String tableName = dataUnit.getName();
            if (!inuseTableName.contains(tableName)) {
                SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
                if (tableName.startsWith(TABLE_PREFIX, 0)) {//delete prefix=ToBeDelete redshift tablename
                    String timestamp = tableName.replace(TABLE_PREFIX, "");
                    timestamp = timestamp.substring(0, timestamp.indexOf('_'));
                    log.info("to be deleted redshift table timestamp is " + timestamp);
                    int distance = (int) (Long.valueOf(df.format(new Date())) - Long.valueOf(timestamp));
                    log.info("redshift table " + dataUnit.getName() + " distance is " + distance + " days");
                    if (distance > 9) {
                        log.info("need delete redshift tablename under tenant is :" + dataUnit.getName());
                        dataUnitProxy.delete(tenant.getId(), dataUnit);
                    }
                } else {//rename redshiftname wait delete
                    String new_tableName = TABLE_PREFIX + df.format(new Date()) + "_" + tableName;
                    log.info("new table name is " + new_tableName);
                    dataUnitProxy.renameTableName(tenant.getId(), dataUnit,
                            new_tableName);
                }
            }
        }
    }

}
