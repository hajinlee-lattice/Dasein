package com.latticeengines.apps.cdl.service.impl;

import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.TenantCleanupService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component("tenantCleanupService")
public class TenantCleanupServiceImpl implements TenantCleanupService {

    private static final Logger log = LoggerFactory.getLogger(TenantCleanupServiceImpl.class);

    @Inject
    private RedshiftService redshiftService;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private MetadataProxy metadataProxy;

    private static final String TABLE_PREFIX = "ToBeDeletedOn_";

    public boolean removeTenantTables(String tenantId) {
        List<String> allRedshiftTable = redshiftService.getTables("");
        List<String> metadataTables = dataCollectionService.getTableNames(tenantId, null, null, null);
        List<String> allTenantId = tenantEntityMgr.getAllTenantId();
        Set<String> existingTenant = new HashSet<>();
        Set<String> allExceptionTable = new HashSet<>();
        for (String tmpTenantId : allTenantId) {
            existingTenant.add(CustomerSpace.parse(tmpTenantId).getTenantId().toLowerCase());
        }
        log.info("metadataTable is " + metadataTables.toString());
        List<String> redshiftTables = getRedshiftTable(allRedshiftTable, tenantId);
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        for (String tableName : redshiftTables) {
            try {
                DataUnit dataUnit = null;
                log.info("need delete redshift tablename under tenant " + tenantId + " is :" + tableName);
                if (existingTenant.contains(tenantId.toLowerCase())) {
                    dataUnit = dataUnitProxy.getByNameAndType(tenantId.toLowerCase(), tableName,
                            DataUnit.StorageType.Redshift);
                }
                if (dataUnit != null) {
                    log.info("dataunit is " + dataUnit.getName());
                    dataUnitProxy.delete(tenantId.toLowerCase(), dataUnit);
                } else {
                    redshiftService.dropTable(tableName);
                }
            } catch (Exception e) {
                allExceptionTable.add(tableName);
                log.warn("deal table " + tableName + "throws exception, Error Message is " + e.getMessage());
            }
        }

        for (String tableName : metadataTables) {
            try {
                if (StringUtils.isNotEmpty(tableName)) {
                    metadataProxy.deleteTable(tenantId, tableName);
                }
            } catch (Exception e) {
                allExceptionTable.add(tableName);
                log.warn("deal table " + tableName + "throws exception, Error Message is " + e.getMessage());
            }
        }

        log.info("all need deleted redshiftTable is " + redshiftTables.toString());
        log.info("all need deleted metadataTable is " + metadataTables.toString());
        log.info("all happen exception tables is : " + allExceptionTable.toString());

        return true;
    }

    private List<String> getRedshiftTable(List<String> redshiftTables, String tenantId) {
        log.info("all redshiftTable number is " + redshiftTables.size() + " , tenantId is " + tenantId);
        List<String> redshiftTablesOfTenant = new ArrayList<String>();
        Pattern patternWithPrefix = Pattern.compile(TABLE_PREFIX.toLowerCase() + "\\d{8}_" + tenantId.toLowerCase() + "_([a-z]+)_(\\d{4}_\\d{2}_\\d{2})_\\d{2}_\\d{2}_\\d{2}_utc$");
        Pattern patternWithOutPrefix = Pattern.compile(tenantId.toLowerCase() + "_([a-z]+)_(\\d{4}_\\d{2}_\\d{2})_\\d{2}_\\d{2}_\\d{2}_utc$");
        for (String redshiftTable : redshiftTables) {
            Matcher matcher = patternWithOutPrefix.matcher(redshiftTable);
            if (matcher.matches()) {
                redshiftTablesOfTenant.add(redshiftTable);
            } else {
                matcher = patternWithPrefix.matcher(redshiftTable);
                if (matcher.matches()) {
                    redshiftTablesOfTenant.add(redshiftTable);
                }
            }
        }
        log.info("redshiftTable of tenant " + tenantId + " : " + JsonUtils.serialize(redshiftTablesOfTenant));
        log.info("redshiftTable number is " + redshiftTables.size());
        return redshiftTablesOfTenant;
    }
}
