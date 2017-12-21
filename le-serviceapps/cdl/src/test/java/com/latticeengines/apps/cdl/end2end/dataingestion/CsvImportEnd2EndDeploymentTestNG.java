package com.latticeengines.apps.cdl.end2end.dataingestion;

import java.io.IOException;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class CsvImportEnd2EndDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private long importedAccounts;
    private long importedContacts;
    private long importedAccounts2;
    private long importedContacts2;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        importData();
        processAnalyze();
        verifyProfile();
    }

    private void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        importedAccounts = importCsv(BusinessEntity.Account, 1);
        importedAccounts2 = importCsv(BusinessEntity.Account, 2);
        importedContacts = importCsv(BusinessEntity.Contact, 1);
        importedContacts2 = importCsv(BusinessEntity.Contact, 2);

        Thread.sleep(2000);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void verifyProfile() throws IOException {
        Map<TableRoleInCollection, Long> expectedCounts = ImmutableMap.of( //
                TableRoleInCollection.BucketedAccount, importedAccounts + importedAccounts2,
                TableRoleInCollection.SortedContact, importedContacts + importedContacts2);
        verifyProfileReport(processAnalyzeAppId, expectedCounts);
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainTestTenant.getId());
        Assert.assertEquals(DataFeed.Status.Active, dataFeed.getStatus());

        verifyStats(BusinessEntity.Account, BusinessEntity.Contact);

        Assert.assertEquals(countInRedshift(BusinessEntity.Account), importedAccounts + importedAccounts2);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), importedContacts + importedContacts2);
    }

}
