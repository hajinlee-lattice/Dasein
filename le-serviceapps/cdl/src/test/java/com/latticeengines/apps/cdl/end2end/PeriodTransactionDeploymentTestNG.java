package com.latticeengines.apps.cdl.end2end;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class PeriodTransactionDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(PeriodTransactionDeploymentTestNG.class);

    @Test(groups = "deployment", enabled = false)
    public void testLaunchPA() throws Exception {
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        verifyNumAttrsInAccount();
        new Thread(() -> {
            createTestSegment1();
            createTestSegment2();
        }).start();
        importData();
        createAction();
        processAnalyze();

        DataCollection.Version active = dataCollectionProxy.getActiveVersion(mainCustomerSpace);
        Table table = dataCollectionProxy.getTable(mainCustomerSpace,
                TableRoleInCollection.ConsolidatedDailyTransaction, active);
        Assert.assertNotNull(table);

        List<Table> periodTables = dataCollectionProxy.getTables(mainCustomerSpace,
                TableRoleInCollection.ConsolidatedPeriodTransaction, active);
        Assert.assertNotNull(periodTables);
    }

    @AfterClass(groups = "deployment", enabled = false)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    private void createAction() {
        Action businessCalendarChangedAction = new Action();
        businessCalendarChangedAction.setType(ActionType.BUSINESS_CALENDAR_CHANGE);
        businessCalendarChangedAction.setActionInitiator(mainCustomerSpace);
        businessCalendarChangedAction.setCreated(new Date());
        businessCalendarChangedAction.setDescription(ActionType.BUSINESS_CALENDAR_CHANGE.getDisplayName());
        actionProxy.createAction(mainCustomerSpace, businessCalendarChangedAction);
    }

    private void importData() throws Exception {
        mockCSVImport(BusinessEntity.Transaction, 1, "Transaction");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Transaction, 2, "Transaction");
        Thread.sleep(2000);
    }

    private void verifyNumAttrsInAccount() {
        String tableName = dataCollectionProxy.getTableName(mainCustomerSpace, BusinessEntity.Account.getServingStore());
        List<ColumnMetadata> cms = metadataProxy.getTableColumns(mainCustomerSpace, tableName);
        Assert.assertTrue(cms.size() < 20000, "Should not have more than 20000 account attributes");
    }
}
