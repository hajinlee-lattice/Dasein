package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.TenantCleanupService;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftPartitionService;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

@Component("tenantCleanupService")
public class TenantCleanupServiceImpl implements TenantCleanupService {

    private static final Logger log = LoggerFactory.getLogger(TenantCleanupServiceImpl.class);

    @Inject
    private RedshiftPartitionService redshiftPartitionService;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private MetadataProxy metadataProxy;

    public boolean removeTenantTables(String customerSpace) {
        List<TableRoleInCollection> servingStoreRoles = getServingStoreRoleList();
        List<String> metadataTables = new ArrayList<String>();
        List<String> allRedshiftTables = new ArrayList<String>();
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
        DataCollectionStatus status = dataCollectionService.getOrCreateDataCollectionStatus(customerSpace, null);
        RedshiftService redshiftService = redshiftPartitionService.getBatchUserService(status.getRedshiftPartition());
        for (String tableName : metadataTables) {
            try {
                if (StringUtils.isNotEmpty(tableName)) {
                    metadataProxy.deleteTable(customerSpace, tableName);
                    List<String> redshiftTables = redshiftService.getTables(tableName);
                    if (CollectionUtils.isNotEmpty(redshiftTables)) {
                        redshiftTables.forEach(redshiftTable -> {
                            allRedshiftTables.add(redshiftTable);
                            DataUnit dataUnit = null;
                            log.info("need delete redshift tablename under tenant " + customerSpace + " is :" + tableName);
                            dataUnit = dataUnitProxy.getByNameAndType(customerSpace, tableName,
                                    DataUnit.StorageType.Redshift);
                            if (dataUnit != null) {
                                log.info("dataunit is " + dataUnit.getName());
                                dataUnitProxy.delete(customerSpace, dataUnit);
                            } else {
                                log.info("no dataunit found, drop redshift table directly");
                                redshiftService.dropTable(tableName);
                            }
                        });
                    }
                }
            } catch (Exception e) {
                allExceptionTable.add(tableName);
                log.warn("deal table " + tableName + "throws exception, Error Message is " + e.getMessage());
            }
        }
        log.info("all need deleted serving metadata table is " + metadataTables.toString());
        log.info("all need deleted redshift table is " + allRedshiftTables.toString());
        log.info("all happen exception tables is : " + allExceptionTable.toString());
        return true;
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
