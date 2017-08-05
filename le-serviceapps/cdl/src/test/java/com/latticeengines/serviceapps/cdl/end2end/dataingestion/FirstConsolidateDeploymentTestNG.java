package com.latticeengines.serviceapps.cdl.end2end.dataingestion;

import static org.testng.Assert.assertEquals;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class FirstConsolidateDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(FirstConsolidateDeploymentTestNG.class);

    @Override
    Logger getLogger() {
        return log;
    }

    @Test(groups = "deployment")
    public void testFirstConsolidate() throws Exception {
        importData();
        consolidate();
        verify();
    }

    private void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockAvroData(0, 300);
        Thread.sleep(2000);
        mockAvroData(300, 200);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void consolidate() {
        log.info("Start consolidating ...");
        ApplicationId appId = cdlProxy.consolidate(mainTestTenant.getId());
        JobStatus completedStatus = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    private void verify() {
        long numAccounts = countTableRole(BusinessEntity.Account.getBatchStore());
        Assert.assertEquals(numAccounts, 300);
        long numContacts = countTableRole(BusinessEntity.Contact.getBatchStore());
        Assert.assertEquals(numContacts, 300);
    }

}
