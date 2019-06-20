package com.latticeengines.apps.cdl.end2end;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ResetImportDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ResetImportDeploymentTestNG.class);

    @BeforeClass(groups = { "manual" })
    public void setup() throws Exception {
        setupEnd2EndTestEnvironment();
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
    }

    @Test(groups = "manual")
    public void runTest() {
        importData();

        DataFeedTask accountTask = dataFeedProxy.getDataFeedTask(mainTestTenant.getId(), "File", "AccountSchema",
                BusinessEntity.Account.name());
        DataFeedTask contactTask = dataFeedProxy.getDataFeedTask(mainTestTenant.getId(), "File", "ContactSchema",
                BusinessEntity.Contact.name());

        Assert.assertNotNull(accountTask);
        Assert.assertNotNull(contactTask);

        List<Action> importActions = actionProxy.getActions(mainTestTenant.getId()).stream()
                .filter(action -> action.getType().equals(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW))
                .collect(Collectors.toList());

        Assert.assertNotNull(importActions);
        Assert.assertTrue(importActions.size() > 0);

        cdlProxy.resetImport(mainTestTenant.getId(), BusinessEntity.Account);
        accountTask = dataFeedProxy.getDataFeedTask(mainTestTenant.getId(), "File", "AccountSchema",
                BusinessEntity.Account.name());
        Assert.assertNull(accountTask);

        cdlProxy.resetImport(mainTestTenant.getId(), null);
        contactTask = dataFeedProxy.getDataFeedTask(mainTestTenant.getId(), "File", "ContactSchema",
                BusinessEntity.Contact.name());
        Assert.assertNull(contactTask);

        importActions = actionProxy.getActions(mainTestTenant.getId()).stream()
                .filter(action -> action.getType().equals(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW))
                .collect(Collectors.toList());
        Assert.assertEquals(0, importActions.size());
    }

    private void importData() {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());

        importData(BusinessEntity.Account, "Account_401_500.csv");

        importData(BusinessEntity.Contact, "Contact_401_500.csv");


    }

}
