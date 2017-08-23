package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_3;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_3;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class SecondProfileDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint("consolidate2");
        verifySecondConsolidateCheckpoint();

        importData();
        profile();
        verifyProfile();

        verifySecondProfileCheckpoint();
    }

    private void importData() throws Exception {
        mockVdbImport(BusinessEntity.Account, ACCOUNT_IMPORT_SIZE_1 + ACCOUNT_IMPORT_SIZE_2, ACCOUNT_IMPORT_SIZE_3);
        mockVdbImport(BusinessEntity.Contact, CONTACT_IMPORT_SIZE_1 + CONTACT_IMPORT_SIZE_2, CONTACT_IMPORT_SIZE_3);
        Thread.sleep(2000);
    }

    private void verifyProfile() {
        int numAccounts = ACCOUNT_IMPORT_SIZE_1 + ACCOUNT_IMPORT_SIZE_2;
        int numContacts = CONTACT_IMPORT_SIZE_1 + CONTACT_IMPORT_SIZE_2;
        verifyProfileReport(profileAppId, 2, numAccounts, numContacts);
        verifyDataFeedStatsu(DataFeed.Status.Active);
        verifyActiveVersion(DataCollection.Version.Green);

        Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()), numAccounts);
        Assert.assertEquals(countTableRole(BusinessEntity.Contact.getBatchStore()), numContacts);
        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
    }

}
