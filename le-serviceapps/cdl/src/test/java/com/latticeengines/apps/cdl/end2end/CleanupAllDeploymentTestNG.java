package com.latticeengines.apps.cdl.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

public class CleanupAllDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(CleanupAllDeploymentTestNG.class);

    private static final int ACCOUNT_IMPORT_SIZE_1 = 500;

    private String customerSpace;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private RedshiftService redshiftService;

    @Inject
    private CDLAttrConfigProxy cdlAttrConfigProxy;

    private Map<BusinessEntity, String> tablename = new HashMap();

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        verifyCleanupAllAttrConfig();
        verifyCleanup();
        processAnalyze();
        verifyProcess();
        verifyCleanupAll();
    }

    private void verifyCleanup() {
        ApplicationId contact_appId = verifyCleanup(BusinessEntity.Contact);
        ApplicationId transaction_appId = verifyCleanup(BusinessEntity.Transaction);
        verifyLock();
        verifyStatus(contact_appId);
        verifyStatus(transaction_appId);
        ApplicationId contactTable_appId = verifyDeleteTable(BusinessEntity.Contact);
        ApplicationId transactionTable_appId = verifyDeleteTable(BusinessEntity.Transaction);
        verifyAction(contactTable_appId, BusinessEntity.Contact);
        verifyAction(transactionTable_appId, BusinessEntity.Transaction);
    }

    private ApplicationId verifyCleanup(BusinessEntity entity) {
        log.info(String.format("clean up all data for entity %s, current action number is %d", entity.toString(),
                actionsNumber));
        log.info("cleaning up all data of " + entity + " ... ");
        tablename.put(entity, dataCollectionProxy.getTableName(customerSpace, entity.getBatchStore()));
        ApplicationId appId = cdlProxy.cleanupAllData(customerSpace, entity, MultiTenantContext.getEmailAddress());
        return appId;

    }

    private void verifyCleanupAllAttrConfig() {
        ApplicationId appId = cdlProxy.cleanupAllAttrConfig(customerSpace, BusinessEntity.Contact,
                MultiTenantContext.getEmailAddress());
        verifyStatus(appId);
        AttrConfigRequest request = cdlAttrConfigProxy.getAttrConfigByEntity(customerSpace, BusinessEntity.Contact,
                false);
        Assert.assertEquals(request.getAttrConfigs().size(), 0);


        log.info("cleaning up all attr config " + customerSpace);
        appId = cdlProxy.cleanupAllAttrConfig(customerSpace, null, MultiTenantContext.getEmailAddress());
        verifyStatus(appId);
        request = cdlAttrConfigProxy.getAttrConfigByEntity(customerSpace, BusinessEntity.Transaction, false);
        Assert.assertEquals(request.getAttrConfigs().size(), 0);
    }

    private void verifyStatus(ApplicationId appId) {
        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(status, JobStatus.COMPLETED);
    }

    private ApplicationId verifyDeleteTable(BusinessEntity entity) {
        log.info("assert the DataCollectionTable and MetadataTable is deleted.");
        Table table = dataCollectionProxy.getTable(customerSpace, entity.getBatchStore());
        assertNull(table);
        log.info("delete tablename is :" + tablename.get(entity));
        table = metadataProxy.getTable(customerSpace, tablename.get(entity));
        assertNull(table);

        log.info("cleaning up all metadata of " + entity + " ... ");
        ApplicationId appId = cdlProxy.cleanupAll(CustomerSpace.parse(mainTestTenant.getId()).toString(), entity,
                MultiTenantContext.getEmailAddress());
        return appId;
    }

    private void verifyAction(ApplicationId appId, BusinessEntity entity) {
        verifyStatus(appId);
        List<DataFeedTask> dfTasks = dataFeedProxy.getDataFeedTaskWithSameEntity(customerSpace, entity.name());
        if (dfTasks != null) {
            assertEquals(dfTasks.size(), 0);
        }
        verifyActionRegistration();
    }

    private void verifyCleanupAll() {
        String tableA = dataCollectionProxy.getTable(customerSpace, BusinessEntity.Account.getBatchStore()).getName();

        log.info("cleaning up all for all entities...");
        ApplicationId appId = cdlProxy.cleanupAll(customerSpace, null, MultiTenantContext.getEmailAddress());
        Assert.expectThrows(RuntimeException.class, () -> verifyCleanup(BusinessEntity.Contact));
        verifyStatus(appId);

        log.info("assert the DataCollectionTable is deleted.");
        assertNull(dataCollectionProxy.getTable(customerSpace, BusinessEntity.Account.getBatchStore()));
        assertNull(dataCollectionProxy.getTable(customerSpace, BusinessEntity.Contact.getBatchStore()));

        List<String> redshiftTables = redshiftService.getTables(tableA);
        Assert.assertTrue(CollectionUtils.isEmpty(redshiftTables),
                String.format("Table %s is still in redshift", tableA));
    }

    private void verifyProcess() {
        runCommonPAVerifications();
        verifyStats(false, BusinessEntity.Account);

        long numAccounts = ACCOUNT_IMPORT_SIZE_1;

        Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()), numAccounts);
//        Assert.assertEquals(countTableRole(BusinessEntity.Product.getBatchStore()), (long) PRODUCT_IMPORT_SIZE_1);
        verifyFailedToCountTableRole(BusinessEntity.Contact.getBatchStore());
        verifyFailedToCountTableRole(TableRoleInCollection.ConsolidatedRawTransaction);

        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
        Assert.assertNull(getTableName(BusinessEntity.Contact.getBatchStore()));

        verifyStatsCubes();
    }

    private void verifyFailedToCountTableRole(TableRoleInCollection role) {
        boolean hasError = false;
        try {
            countTableRole(role);
        } catch (AssertionError e) {
            hasError = e.getMessage().contains("Cannot find table");
        }
        Assert.assertTrue(hasError, "Should throw error when counting batch store of " + role);
    }

    private void verifyStatsCubes() {
        StatisticsContainer container = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertFalse(container.getStatsCubes().containsKey(BusinessEntity.Contact.name()),
                "Should not have contact's stats cube.");
        Assert.assertFalse(container.getStatsCubes().containsKey(BusinessEntity.PurchaseHistory.name()),
                "Should not have purchase history's stats cube.");
    }

    private void verifyLock() {
        boolean hasError = false;
        try {
            processAnalyze();
        } catch (RuntimeException e) {
            hasError = e.getMessage().contains("can't start processAnalyze workflow");
        }
        Assert.assertTrue(hasError);
    }

}