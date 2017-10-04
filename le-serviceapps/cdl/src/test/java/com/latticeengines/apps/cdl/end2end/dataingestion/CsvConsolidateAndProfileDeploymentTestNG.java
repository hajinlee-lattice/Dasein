package com.latticeengines.apps.cdl.end2end.dataingestion;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class CsvConsolidateAndProfileDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private long importedAccounts;
    private long importedContacts;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        uploadAccountCSV();
        importData();
        consolidate();
        profile();
        verifyProfile();
    }

    private void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        importedAccounts = importCsv(BusinessEntity.Account, 1);
        importedContacts = importCsv(BusinessEntity.Contact, 1);
        Thread.sleep(2000);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void verifyProfile() throws IOException {
        Map<TableRoleInCollection, Long> expectedCounts = ImmutableMap.of( //
                TableRoleInCollection.BucketedAccount, importedAccounts,
                TableRoleInCollection.SortedContact, importedContacts);
        verifyProfileReport(profileAppId, expectedCounts);
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainTestTenant.getId());
        Assert.assertEquals(DataFeed.Status.Active, dataFeed.getStatus());

        verifyStats(BusinessEntity.Account, BusinessEntity.Contact);

        Assert.assertEquals(countInRedshift(BusinessEntity.Account), importedAccounts);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), importedContacts);
    }

}
