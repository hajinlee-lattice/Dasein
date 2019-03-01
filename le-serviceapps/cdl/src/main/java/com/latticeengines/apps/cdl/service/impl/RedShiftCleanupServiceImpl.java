package com.latticeengines.apps.cdl.service.impl;

import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataFeedEntityMgr;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.RedShiftCleanupService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
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

    @Value("${cdl.redshift.cleanup.start:false}")
    private boolean cleanupFlag;

    private final static String TABLE_PREFIX = "ToBeDeletedOn_";

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
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        for (DataUnit dataUnit : dataUnits) {
            log.info("dataUnit = " + dataUnit.getName());
            String tableName = dataUnit.getName();
            if (!inuseTableName.contains(tableName)) {
                if (tableName.startsWith(TABLE_PREFIX, 0)) {//delete prefix=ToBeDelete redshift tablename
                    String timestamp = tableName.replace(TABLE_PREFIX, "");
                    timestamp = timestamp.substring(0, timestamp.indexOf('_'));
                    log.info("to be deleted redshift table timestamp is " + timestamp);
                    boolean del = false;
                    try {
                        Date fDate = df.parse(df.format(new Date()));
                        Date oDate = df.parse(timestamp);
                        long days = ChronoUnit.DAYS.between(oDate.toInstant(), fDate.toInstant());
                        log.info("redshift table " + dataUnit.getName() + " distance is " + days + " days");
                        if (days > 9)
                            del = true;
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                    if (del) {
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

    public boolean removeUnusedTables() {
        if (!cleanupFlag) {
            log.warn("the cleanupFlag is " + cleanupFlag);
            return true;
        }
        List<String> allRedshiftTable = redshiftService.getTables("");
        List<String> allTenantId = tenantEntityMgr.findAllTenantId();
        Set<String> allMissTenant = new HashSet<>();
        List<String> existingTenant = new ArrayList<>();
        Set<String> allMissTable = new HashSet<>();
        for (String tenantId : allTenantId) {
            existingTenant.add(CustomerSpace.parse(tenantId).getTenantId().toLowerCase());
        }
        List<String> metadataTable = dataCollectionService.getAllTableNames();
        log.info("metadataTable is " + metadataTable.toString());
        allRedshiftTable.removeAll(metadataTable);
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        for (String tableName : allRedshiftTable) {
            if (tableName.startsWith(TABLE_PREFIX.toLowerCase(), 0)) {//delete prefix=ToBeDelete redshift tablename
                String formatstr = tableName.replace(TABLE_PREFIX.toLowerCase(), "");
                String timestamp = formatstr.substring(0, formatstr.indexOf('_'));
                String regx = "_([a-z]+)_\\d{4}_\\d{2}_\\d{2}_\\d{2}_\\d{2}_\\d{2}_utc$";
                String tenantName = formatstr.replaceAll(regx, "");
                log.info("to be deleted redshift table timestamp is " + timestamp);
                boolean del = false;
                try {
                    Date fDate = df.parse(df.format(new Date()));
                    Date oDate = df.parse(timestamp);
                    long days = ChronoUnit.DAYS.between(oDate.toInstant(), fDate.toInstant());
                    log.info("redshift table " + tableName + " distance is " + days + " days");
                    if (days > 9)
                        del = true;
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
                if (del) {
                    DataUnit dataUnit = null;
                    log.info("need delete redshift tablename under tenant " + tenantName + " is :" + tableName);
                    if (existingTenant.contains(tenantName)) {
                        dataUnit = dataUnitProxy.getByNameAndType(tenantName, tableName,
                                DataUnit.StorageType.Redshift);
                    }
                    if (dataUnit != null) {
                        log.info("dataunit is " + dataUnit.getName());
                        dataUnitProxy.delete(tenantName, dataUnit);
                    } else {
                        redshiftService.dropTable(tableName);
                    }
                }
            } else {
                Pattern p = Pattern.compile("([a-z0-9_]+)_([a-z]+)_(\\d{4}_\\d{2}_\\d{2})_\\d{2}_\\d{2}_\\d{2}_utc$");
                Matcher matcher = p.matcher(tableName);
                if (matcher.matches()) {
                    String tenantName = matcher.group(1);
                    String createTime = matcher.group(3).replaceAll("_", "");
                    log.info("createTime is " + createTime);
                    boolean del = false;
                    try {
                        Date fDate = df.parse(df.format(new Date()));
                        Date oDate = df.parse(createTime);
                        long days = ChronoUnit.DAYS.between(oDate.toInstant(), fDate.toInstant());
                        log.info("time distance is " + days);
                        if (days > 7)//avoid to delete the table belongs to local test tenant
                            del = true;
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }

                    if (del) {
                        allMissTable.add(tableName);
                        DataUnit dataUnit = null;
                        if (!existingTenant.contains(tenantName)) {//deal with the table which tenant was miss
                            allMissTenant.add(tenantName);
                            log.info("RedshiftTable " + tableName + " need to deleted because tenant " + tenantName +
                                    " was missed");
                        } else {
                            log.info("RedshiftTable " + tableName + " need to deleted because we cannot find it in Db");
                            dataUnit = dataUnitProxy.getByNameAndType(tenantName, tableName,
                                    DataUnit.StorageType.Redshift);
                        }

                        String new_tableName = TABLE_PREFIX + df.format(new Date()) + "_" + tableName;
                        log.info("new table name is " + new_tableName);
                        if (dataUnit != null) {
                            dataUnitProxy.renameTableName(tenantName, dataUnit, new_tableName);
                        } else {
                            redshiftService.renameTable(tableName, new_tableName);
                        }
                    }
                }
            }
        }
        log.info("all miss tenant is " + allMissTenant.toString());
        log.info("all need deleted table is " + allMissTable.toString());

        return true;
    }
}
