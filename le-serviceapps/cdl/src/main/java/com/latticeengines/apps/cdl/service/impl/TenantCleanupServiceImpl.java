package com.latticeengines.apps.cdl.service.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.TenantCleanupService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

@Component("tenantCleanupService")
public class TenantCleanupServiceImpl implements TenantCleanupService {

    private static final Logger log = LoggerFactory.getLogger(TenantCleanupServiceImpl.class);

    @Inject
    private RedshiftService redshiftService;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private MetadataProxy metadataProxy;

    private static final String TABLE_PREFIX = "ToBeDeletedOn_";

    public boolean removeTenantTables(String customerSpace) {
        List<TableRoleInCollection> servingStoreRoles = getServingStoreRoleList();
        List<String> metadataTables = new ArrayList<String>();
        servingStoreRoles.forEach(servingStoreRole -> {
            List<String> tables = dataCollectionService.getTableNames(customerSpace, null, servingStoreRole, null);
            log.info("Serving metadataTable with role " + servingStoreRole.name() + " : " + tables.toString());
            if (!tables.isEmpty()) {
                log.info("metadataTable is " + metadataTables.toString());
                metadataTables.addAll(tables);
            }
        });

        List<String> allExceptionTable = new ArrayList<String>();
        log.info("metadataTable is " + metadataTables.toString());
        List<String> allRedshiftTables = redshiftService.getTables(customerSpace + "_");
        List<String>redshiftTables = getRedshiftTable(allRedshiftTables, customerSpace);
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        for (String tableName : redshiftTables) {
            try {
                DataUnit dataUnit = null;
                log.info("need delete redshift tablename under tenant " + customerSpace + " is :" + tableName);
                dataUnit = dataUnitProxy.getByNameAndType(customerSpace.toLowerCase(), tableName,
                        DataUnit.StorageType.Redshift);
                if (dataUnit != null) {
                    log.info("dataunit is " + dataUnit.getName());
                    dataUnitProxy.delete(customerSpace.toLowerCase(), dataUnit);
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
                    metadataProxy.deleteTable(customerSpace, tableName);
                }
            } catch (Exception e) {
                allExceptionTable.add(tableName);
                log.warn("deal table " + tableName + "throws exception, Error Message is " + e.getMessage());
            }
        }
        log.info("all need deleted redshiftTable is " + redshiftTables.toString());
        log.info("all need deleted serving metadataTable is " + metadataTables.toString());
        log.info("all happen exception tables is : " + allExceptionTable.toString());
        return true;
    }

    List<String> getRedshiftTable(List<String> redshiftTables, String customerSpace) {
        log.info("all redshiftTable match " + customerSpace + " number is " + redshiftTables.size() + " , customerSpace is " + customerSpace);
        List<String>result = new ArrayList<String>();
        Pattern pattern = Pattern.compile(customerSpace.toLowerCase() + "_([a-z]+)_(\\d{4}_\\d{2}_\\d{2})_\\d{2}_\\d{2}_\\d{2}_utc$");
        for (String redshiftTable : redshiftTables) {
            Matcher matcher = pattern.matcher(redshiftTable);
            if (matcher.matches()) {
                result.add(redshiftTable);
            }
        }
        log.info("redshiftTable of tenant " + customerSpace + " : " + JsonUtils.serialize(redshiftTables));
        log.info("redshiftTable number is " + redshiftTables.size());
        return result;
    }

    protected List<TableRoleInCollection> getServingStoreRoleList() {
        return Arrays.asList(
                BusinessEntity.Account.getServingStore(),
                BusinessEntity.Contact.getServingStore(),
                BusinessEntity.Product.getServingStore(),
                BusinessEntity.Transaction.getServingStore(),
                BusinessEntity.PeriodTransaction.getServingStore(),
                BusinessEntity.PurchaseHistory.getServingStore(),
                BusinessEntity.DepivotedPurchaseHistory.getServingStore(),
                BusinessEntity.CuratedAccount.getServingStore(),
                BusinessEntity.AnalyticPurchaseState.getServingStore(),
                BusinessEntity.Rating.getServingStore(),
                BusinessEntity.LatticeAccount.getServingStore(),
                BusinessEntity.ProductHierarchy.getServingStore()
        );
    }
}
