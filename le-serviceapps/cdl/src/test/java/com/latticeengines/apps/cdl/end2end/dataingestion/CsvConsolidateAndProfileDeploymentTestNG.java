package com.latticeengines.apps.cdl.end2end.dataingestion;

import java.io.IOException;

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
        importedAccounts = mockCsvImport(BusinessEntity.Account, 1);
        importedContacts = mockCsvImport(BusinessEntity.Contact, 1);
        Thread.sleep(2000);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void verifyProfile() throws IOException {
        verifyReport(profileAppId, 2, importedAccounts, importedContacts);
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainTestTenant.getId());
        Assert.assertEquals(DataFeed.Status.Active, dataFeed.getStatus());

        verifyStats(BusinessEntity.Account, BusinessEntity.Contact);

        Assert.assertEquals(countInRedshift(BusinessEntity.Account), importedAccounts);
        // Assert.assertEquals(countInRedshift(BusinessEntity.Contact), importedContacts);
    }

}
