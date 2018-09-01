package com.latticeengines.apps.cdl.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.List;

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
import com.latticeengines.domain.exposed.workflow.JobStatus;
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

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();

        verifyCleanup(BusinessEntity.Contact);
        verifyCleanup(BusinessEntity.Transaction);
        processAnalyze();
        verifyProcess();
        verifyCleanupAll();
    }

    private void verifyCleanup(BusinessEntity entity) {
        log.info(String.format("clean up all data for entity %s, current action number is %d", entity.toString(),
                actionsNumber));

        String tableName = dataCollectionProxy.getTableName(customerSpace, entity.getBatchStore());

        log.info("cleaning up all data of " + entity + " ... ");
        ApplicationId appId = cdlProxy.cleanupAllData(customerSpace, entity, MultiTenantContext.getEmailAddress());
        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(status, JobStatus.COMPLETED);

        log.info("assert the DataCollectionTable and MetadataTable is deleted.");
        Table table = dataCollectionProxy.getTable(customerSpace, entity.getBatchStore());
        assertNull(table);
        table = metadataProxy.getTable(customerSpace, tableName);
        assertNull(table);

        log.info("cleaning up all metadata of " + entity + " ... ");
        appId = cdlProxy.cleanupAll(CustomerSpace.parse(mainTestTenant.getId()).toString(), entity,
                MultiTenantContext.getEmailAddress());
        status = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(status, JobStatus.COMPLETED);
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
        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(status, JobStatus.COMPLETED);

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

}