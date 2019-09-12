package com.latticeengines.apps.cdl.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

public class CleanupAllDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(CleanupAllDeploymentTestNG.class);

    private static final int ACCOUNT_IMPORT_SIZE_1 = 900;

    private String customerSpace;

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
        if (isLocalEnvironment()) {
            processAnalyzeSkipPublishToS3();
        } else {
            processAnalyze();
        }
        Assert.assertEquals(1, verifyAction());
        verifyProcess();
        verifyCleanupAll();
    }

    private void verifyCleanup() {
        verifyCleanup(BusinessEntity.Contact);
        verifyCleanup(BusinessEntity.Transaction);
        Assert.assertEquals(2, verifyAction());
        importData();
    }

    void processAnalyzeSkipPublishToS3() {
        ProcessAnalyzeRequest request = new ProcessAnalyzeRequest();
        request.setSkipPublishToS3(true);
        request.setSkipDynamoExport(true);
        processAnalyze(request);
    }

    private void verifyCleanup(BusinessEntity entity) {
        log.info(String.format("clean up all data for entity %s, current action number is %d", entity.toString(),
                actionsNumber));
        log.info("cleaning up all data of " + entity + " ... ");
        tablename.put(entity, dataCollectionProxy.getTableName(customerSpace, entity.getBatchStore()));
        cdlProxy.cleanupAllByAction(customerSpace, entity, MultiTenantContext.getEmailAddress());
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

    private int verifyAction() {
        List<Action> actions = actionProxy.getActionsByOwnerId(customerSpace, null);
        List<Long> cleanupActions =
                actions.stream().filter(action -> ActionType.DATA_REPLACE.equals(action.getType())).map(Action::getPid).filter(Objects::nonNull) //
                        .collect(Collectors.toList());
        return cleanupActions.size();
    }

    private void verifyCleanupAll() {
        String tableA = dataCollectionProxy.getTable(customerSpace, BusinessEntity.Account.getBatchStore()).getName();

        log.info("cleaning up all for all entities...");
        ApplicationId appId = cdlProxy.cleanupAll(customerSpace, null, MultiTenantContext.getEmailAddress());
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
        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
        Assert.assertNotNull(getTableName(BusinessEntity.Contact.getBatchStore()));
        verifyStatsCubes();
    }

    private void verifyStatsCubes() {
        StatisticsContainer container = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertTrue(container.getStatsCubes().containsKey(BusinessEntity.Contact.name()),
                "Should have contact's stats cube.");
        Assert.assertTrue(container.getStatsCubes().containsKey(BusinessEntity.PurchaseHistory.name()),
                "Should have purchase history's stats cube.");
    }

    private void importData() {
        mockCSVImport(BusinessEntity.Contact, 2, "DefaultSystem_ContactData");
    }

}
