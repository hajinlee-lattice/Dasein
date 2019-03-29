package com.latticeengines.apps.cdl.service.impl;

import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
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

import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.RedShiftCleanupService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

@Component("redShiftCleanupService")
public class RedShiftCleanupServiceImpl implements RedShiftCleanupService {

    private static final Logger log = LoggerFactory.getLogger(RedShiftCleanupServiceImpl.class);

    @Inject
    private RedshiftService redshiftService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Value("${cdl.redshift.cleanup.start:false}")
    private boolean cleanupFlag;

    @Value("${cdl.redshift.cleanup.table.remain.day:7L}")
    private Long retentionInDays;

    private static final String TABLE_PREFIX = "ToBeDeletedOn_";

    public boolean removeUnusedTables() {
        if (!cleanupFlag) {
            log.warn("the cleanupFlag is " + cleanupFlag);
            return true;
        }
        List<String> allRedshiftTable = redshiftService.getTables("");
        List<String> allTenantId = tenantEntityMgr.getAllTenantId();
        Set<String> allMissTenant = new HashSet<>();
        Set<String> existingTenant = new HashSet<>();
        Set<String> allMissTable = new HashSet<>();
        Set<String> allExceptionTable = new HashSet<>();
        for (String tenantId : allTenantId) {
            existingTenant.add(CustomerSpace.parse(tenantId).getTenantId().toLowerCase());
        }
        List<String> metadataTables = dataCollectionService.getAllTableNames();
        log.info("metadataTable is " + metadataTables.toString());
        List<String> unUsedRedshiftTable = getUnusedTable(allRedshiftTable, metadataTables);
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        for (String tableName : unUsedRedshiftTable) {
            try {
                if (tableName.startsWith(TABLE_PREFIX.toLowerCase(), 0)) {//delete prefix=ToBeDelete redshift tablename
                    String formatStr = tableName.replace(TABLE_PREFIX.toLowerCase(), "");
                    String timestamp = formatStr.substring(0, formatStr.indexOf('_'));
                    String regx = "_([a-z]+)_\\d{4}_\\d{2}_\\d{2}_\\d{2}_\\d{2}_\\d{2}_utc$";
                    formatStr = formatStr.replace(timestamp + '_', "");
                    String tenantName = formatStr.replaceAll(regx, "");
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
                            if (days > retentionInDays)//avoid to delete the table belongs to local test tenant
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

                            String tablePrefix = TABLE_PREFIX + df.format(new Date()) + "_";
                            log.info("new table prefix is " + tablePrefix);
                            if (dataUnit != null) {
                                String new_tableName = tablePrefix + dataUnit.getName();
                                log.info("new table name is " + new_tableName);
                                dataUnitProxy.renameTableName(tenantName, dataUnit, new_tableName);
                            } else {
                                String new_tableName = tablePrefix + tableName;
                                log.info("new table name is " + new_tableName);
                                redshiftService.renameTable(tableName, new_tableName);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                allExceptionTable.add(tableName);
                log.warn("deal table " + tableName + "throws exception, Error Message is " + e.getMessage());
            }
        }
        log.info("all miss tenant is " + allMissTenant.toString());
        log.info("all need deleted table is " + allMissTable.toString());
        log.info("all happen exception tables is : " + allExceptionTable.toString());

        return true;
    }

    private List<String> getUnusedTable(List<String> redshiftTables, List<String> metadataTables) {
        log.info("all redshiftTable number is " + redshiftTables.size() + " , all metadataTables number is " + metadataTables.size());
        Set<String> unMappingTables = new HashSet<>();
        for (String metadataTable : metadataTables) {
            metadataTable = metadataTable.toLowerCase();
            if (redshiftTables.contains(metadataTable)) {
                redshiftTables.remove(metadataTable);
            } else {
                unMappingTables.add(metadataTable);
            }
        }
        log.info("those metadataTable cannot find in Redshift:" + JsonUtils.serialize(unMappingTables));
        log.info(" unUsed redshiftTable number is " + redshiftTables.size());
        return redshiftTables;
    }
}
