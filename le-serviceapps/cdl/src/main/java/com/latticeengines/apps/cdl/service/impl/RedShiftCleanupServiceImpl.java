package com.latticeengines.apps.cdl.service.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataFeedEntityMgr;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.RedShiftCleanupService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
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

    public boolean removeUnusedRedshiftTable() {
        List<SimpleDataFeed> allSimpleDataFeeds = dataFeedEntityMgr.getAllSimpleDataFeeds();
        if (CollectionUtils.isEmpty(allSimpleDataFeeds)) {
            return true;
        }
        List<Tenant> tenantList = new ArrayList<>();
        for (SimpleDataFeed simpleDataFeed : allSimpleDataFeeds) {
            Tenant tenant = simpleDataFeed.getTenant();
            String prefix = CustomerSpace.parse(tenant.getId()).getTenantId();
            log.info("shorten customerspace is " + prefix);
            if (tenantList.contains(tenant))
                continue;
            log.info("current tenant is :" + tenant.getName());
            List<Tenant> relatedTenants = tenantEntityMgr.findByNameStartingWith(prefix);
            log.info("related tenants is :" + JsonUtils.serialize(relatedTenants));
            tenantList.addAll(relatedTenants);
            this.dropTableByTenant(relatedTenants, prefix);
        }
        //cleanup RedshiftTable without tenant
        dealUnusedRedshiftTable();
        //drop table without dataUnit but also renamed over expired
        deleteTable();
        return true;
    }

    private void dropTableByTenant(List<Tenant> tenants, String prefix) {
        try {
            // 1. get all inused tablename in this tenant_id
            Set<String> tableNames = new HashSet<>();
            for (Tenant curTenant : tenants) {
                MultiTenantContext.setTenant(curTenant);
                log.info("tenant name is : " + curTenant.getName());
                String customerspace = MultiTenantContext.getCustomerSpace().toString();
                DataCollection dataCollection = dataCollectionService.getDefaultCollection(customerspace);
                DataCollection.Version activeVersion = dataCollection.getVersion();
                DataCollection.Version inactiveVersion = activeVersion.complement();
                Set<String> inUsedTableNames = new HashSet<>();
                inUsedTableNames.addAll(dataCollectionService.getTableNames(customerspace, dataCollection.getName(), null, activeVersion));
                inUsedTableNames.addAll(dataCollectionService.getTableNames(customerspace, dataCollection.getName(), null, inactiveVersion));
                tableNames.addAll(inUsedTableNames);
                cleanupTable(curTenant, new ArrayList<>(inUsedTableNames));
            }
            log.info("inuse tableNames:" + tableNames.toString());
            // 2.rename table without dataUnit
            cleanupTableByPrefix(prefix, new ArrayList<>(tableNames));
        } catch (Exception e) {
            log.error(e.toString());
        }
    }

    private void cleanupTableByPrefix(String prefix, List<String> inusedTablename) {
        List<String> redshift_table = redshiftService.getTables(prefix);
        Set<String> renameTableName = new HashSet<>();
        redshift_table.removeAll(inusedTablename);
        if (CollectionUtils.isEmpty(redshift_table)) {
            return;
        }
        for (String simpleTable : redshift_table) {
            log.info("drop table : " + simpleTable);
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
            //rename redshiftname wait delete
            String new_tableName = TABLE_PREFIX + df.format(new Date()) + "_" + simpleTable;
            log.info("new table name is " + new_tableName);
            renameTableName.add(simpleTable);
            redshiftService.renameTable(simpleTable, new_tableName);
        }
        log.info("need rename tableName : " + renameTableName.toString());
    }

    private void deleteTable() {
        List<String> toBeDeletedTable = redshiftService.getTables(TABLE_PREFIX);
        Set<String> deleteTableName = new HashSet<>();
        for (String tableName : toBeDeletedTable) {
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
            String timestamp = tableName.replace(TABLE_PREFIX.toLowerCase(), "");
            timestamp = timestamp.substring(0, timestamp.indexOf('_'));
            log.info("to be deleted redshift table timestamp is " + timestamp);
            int distance = (int) (Long.valueOf(df.format(new Date())) - Long.valueOf(timestamp));
            log.info("redshift table " + tableName + " distance is " + distance + " days");
            if (distance > 9) {
                log.info("need delete redshift tablename under tenant is :" + tableName);
                deleteTableName.add(tableName);
                redshiftService.dropTable(tableName);
            }
        }
        log.info("deleteTable() get tablename is " + deleteTableName.toString());
    }

    public void dealUnusedRedshiftTable() {
        List<String> allRedshiftTable = redshiftService.getTables("");
        List<String> allTenantId = tenantEntityMgr.findAllTenantId();
        Set<String> allMissTenant = new HashSet<>();
        Set<String> existingTenant = new HashSet<>();
        Set<String> allMissTable = new HashSet<>();
        for (String tenantId : allTenantId) {
            existingTenant.add(CustomerSpace.parse(tenantId).getTenantId().toLowerCase());
        }
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        for (String tableName : allRedshiftTable) {
            if (tableName.startsWith(TABLE_PREFIX.toLowerCase()))
                continue;
            Pattern p = Pattern.compile("([a-z0-9_]+)_([a-z]+)_\\d{4}_\\d{2}_\\d{2}_\\d{2}_\\d{2}_\\d{2}_utc$");
            Matcher matcher = p.matcher(tableName);
            if (matcher.find()) {
                String regx = "_([a-z]+)_\\d{4}_\\d{2}_\\d{2}_\\d{2}_\\d{2}_\\d{2}_utc$";
                String tenantName = tableName.replaceAll(regx, "");
                log.info("redshiftTable name is " + tableName);
                log.info("tenant name is " + tenantName);
                if (!existingTenant.contains(tenantName)) {
                    allMissTenant.add(tenantName);
                    allMissTable.add(tableName);
                    String new_tableName = TABLE_PREFIX + df.format(new Date()) + "_" + tableName;
                    log.info("new table name is " + new_tableName);
                    redshiftService.renameTable(tableName, new_tableName);
                }
            }
        }
        log.info("all miss tenant is " + allMissTenant.toString());
        log.info("all miss table is " + allMissTable.toString());
    }
}
